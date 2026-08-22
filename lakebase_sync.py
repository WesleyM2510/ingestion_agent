"""
Lakebase Synced Tables Tool
=============================
Creates and manages Lakebase synced tables via /api/2.0/database/synced_tables.

Sync modes:
  - SNAPSHOT:   One-time full copy, no incremental updates.
  - TRIGGERED:  On-demand or scheduled updates via Databricks Workflows.
  - CONTINUOUS: Real-time streaming with seconds of latency.

Prerequisites:
  - Source tables must have Change Data Feed enabled:
    ALTER TABLE <table> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)
  - Target must be a MANAGED_ONLINE_CATALOG (Lakebase)

Usage:
    from lakebase_sync import LakebaseSyncManager

    mgr = LakebaseSyncManager(profile="cielo_demo")

    # Dry run
    mgr.create_syncs(
        source_tables=["cielo_demo.default.my_table"],
        online_catalog="cielo_postgress",
        primary_key=["pk"],
        sync_mode="triggered",
        dry_run=True,
    )

    # Execute
    mgr.create_syncs(..., dry_run=False)
"""

import json
import os
import subprocess
from dataclasses import dataclass
from enum import Enum


class SyncMode(str, Enum):
    SNAPSHOT = "SNAPSHOT"
    TRIGGERED = "TRIGGERED"
    CONTINUOUS = "CONTINUOUS"


@dataclass
class SyncResult:
    name: str
    src_table: str
    pipeline_id: str
    sync_mode: str
    state: str
    error: str = ""


class LakebaseSyncManager:

    def __init__(self, profile: str = "cielo_demo"):
        self.profile = profile

    def _api(self, method: str, path: str, data: dict = None) -> dict:
        cmd = ["databricks", "api", method, path]
        if data:
            cmd += ["--json", json.dumps(data)]
        env = os.environ.copy()
        env["DATABRICKS_CONFIG_PROFILE"] = self.profile
        try:
            result = subprocess.check_output(cmd, env=env, stderr=subprocess.PIPE)
            return json.loads(result.decode()) if result.strip() else {}
        except subprocess.CalledProcessError as e:
            return {"error": e.stderr.decode().strip()}

    def _run_sql(self, stmt: str) -> tuple[bool, str]:
        resp = self._api("post", "/api/2.0/sql/statements", {
            "warehouse_id": self._warehouse_id,
            "statement": stmt,
            "wait_timeout": "50s",
        })
        state = resp.get("status", {}).get("state", "?")
        if state == "SUCCEEDED":
            return True, "OK"
        err = resp.get("status", {}).get("error", {}).get("message", state)
        return False, err

    @property
    def _warehouse_id(self) -> str:
        if not hasattr(self, "_wh_id"):
            data = self._api("get", "/api/2.0/sql/warehouses")
            for wh in data.get("warehouses", []):
                if wh.get("state") == "RUNNING":
                    self._wh_id = wh["id"]
                    return self._wh_id
            self._wh_id = data.get("warehouses", [{}])[0].get("id", "")
        return self._wh_id

    # ── Discovery ──────────────────────────────────────────────────────

    def list_online_catalogs(self) -> list[dict]:
        data = self._api("get", "/api/2.0/unity-catalog/catalogs")
        return [
            {"name": c["name"], "connection": c.get("connection_name", "")}
            for c in data.get("catalogs", [])
            if c.get("catalog_type") == "MANAGED_ONLINE_CATALOG"
        ]

    def list_syncs(self) -> list[dict]:
        data = self._api("get", "/api/2.0/pipelines")
        syncs = []
        for p in data.get("statuses", []):
            pid = p.get("pipeline_id", "")
            detail = self._api("get", f"/api/2.0/pipelines/{pid}")
            spec = detail.get("spec", {})
            md = spec.get("managed_definition", {}).get("database_table_sync", {})
            for sink in md.get("sinks", []):
                syncs.append({
                    "pipeline_id": pid,
                    "name": p.get("name", ""),
                    "state": detail.get("state", "?"),
                    "src_table": sink.get("src_table", ""),
                    "dest_table": sink.get("dest_table_uc_name", ""),
                    "primary_key": sink.get("primary_key", []),
                    "continuous": spec.get("continuous", False),
                })
        return syncs

    def get_sync(self, synced_table_name: str) -> dict:
        """Get sync status. synced_table_name = catalog.schema.table"""
        return self._api("get", f"/api/2.0/database/synced_tables/{synced_table_name}")

    # ── CDF Enablement ─────────────────────────────────────────────────

    def enable_cdf(self, source_tables: list[str]) -> dict[str, str]:
        """Enable Change Data Feed on source tables (required for syncs)."""
        results = {}
        for t in source_tables:
            ok, msg = self._run_sql(
                f"ALTER TABLE {t} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
            )
            results[t] = "OK" if ok else msg
        return results

    # ── Sync Creation ──────────────────────────────────────────────────

    def create_syncs(
        self,
        source_tables: list[str],
        online_catalog: str,
        storage_catalog: str = None,
        storage_schema: str = None,
        dest_schema: str = "public",
        primary_key: list[str] = None,
        dest_table_names: dict[str, str] = None,
        sync_mode: SyncMode | str = SyncMode.TRIGGERED,
        enable_cdf_auto: bool = True,
        dry_run: bool = True,
    ) -> list[SyncResult]:
        """
        Create Lakebase synced tables.

        Args:
            source_tables:    Fully-qualified source table names (catalog.schema.table)
            online_catalog:   Target Lakebase online catalog name
            storage_catalog:  UC catalog for pipeline intermediate files (checkpoints, logs).
                              Required for managed postgres catalogs. Sets pipeline catalog.
            storage_schema:   UC schema for pipeline intermediate files.
                              Required for managed postgres catalogs. Sets pipeline schema.
            dest_schema:      Destination schema in online catalog (default: "public")
            primary_key:      PK column(s) for the sync
            dest_table_names: Optional src_table -> custom dest name mapping
            sync_mode:        SNAPSHOT | TRIGGERED | CONTINUOUS
            enable_cdf_auto:  Auto-enable CDF on source tables (default True)
            dry_run:          Preview only, no changes
        """
        if isinstance(sync_mode, str):
            sync_mode = SyncMode(sync_mode.upper())
        primary_key = primary_key or ["pk"]
        dest_table_names = dest_table_names or {}

        # Build sync plan
        plan = []
        for src in source_tables:
            dest_name = dest_table_names.get(src, src.split(".")[-1])
            full_dest = f"{online_catalog}.{dest_schema}.{dest_name}"
            plan.append((src, full_dest, dest_name))

        if dry_run:
            print("=" * 70)
            print("DRY RUN — No changes will be made")
            print("=" * 70)
            print(f"  Sync Mode:       {sync_mode.value}")
            print(f"  Online Catalog:  {online_catalog}")
            print(f"  Storage Catalog: {storage_catalog or '(auto)'}")
            print(f"  Storage Schema:  {storage_schema or '(auto)'}")
            print(f"  Dest Schema:     {dest_schema}")
            print(f"  Primary Key:     {primary_key}")
            print(f"  Auto CDF:        {enable_cdf_auto}")
            print("-" * 70)
            for i, (src, dest, _) in enumerate(plan, 1):
                print(f"  [{i}] {src}  ->  {dest}")
            print("=" * 70)
            print(f"Total: {len(plan)} sync(s). Run with dry_run=False to execute.")
            return []

        # Enable CDF
        if enable_cdf_auto:
            print("Enabling CDF on source tables...")
            cdf_results = self.enable_cdf(source_tables)
            for t, r in cdf_results.items():
                print(f"  {t.split('.')[-1]}: {r}")

        # Create syncs
        results = []
        for src, full_dest, _ in plan:
            spec = {
                "source_table_full_name": src,
                "primary_key_columns": primary_key,
                "scheduling_policy": sync_mode.value,
            }
            if storage_catalog or storage_schema:
                spec["new_pipeline_spec"] = {}
                if storage_catalog:
                    spec["new_pipeline_spec"]["storage_catalog"] = storage_catalog
                if storage_schema:
                    spec["new_pipeline_spec"]["storage_schema"] = storage_schema

            payload = {"name": full_dest, "spec": spec}

            print(f"Creating [{sync_mode.value}] {src} -> {full_dest} ...", end=" ")
            resp = self._api("post", "/api/2.0/database/synced_tables", payload)

            if "error" in resp:
                print("FAILED")
                print(f"  {resp['error'][:200]}")
                results.append(SyncResult(full_dest, src, "", sync_mode.value, "FAILED", resp["error"]))
                continue

            status = resp.get("data_synchronization_status", {})
            pid = status.get("pipeline_id", "?")
            state = status.get("detailed_state", "?")
            print(f"OK (pipeline={pid})")
            results.append(SyncResult(full_dest, src, pid, sync_mode.value, state))

        return results

    # ── Management ─────────────────────────────────────────────────────

    def start_sync(self, pipeline_id: str, full_refresh: bool = False) -> dict:
        return self._api("post", f"/api/2.0/pipelines/{pipeline_id}/updates", {
            "full_refresh": full_refresh,
        })

    def stop_sync(self, pipeline_id: str) -> dict:
        return self._api("post", f"/api/2.0/pipelines/{pipeline_id}/stop", {})

    def delete_sync(self, synced_table_name: str) -> dict:
        """Delete by synced table name (catalog.schema.table)."""
        return self._api("delete", f"/api/2.0/database/synced_tables/{synced_table_name}")

    def get_pipeline_status(self, pipeline_id: str) -> dict:
        data = self._api("get", f"/api/2.0/pipelines/{pipeline_id}")
        return {
            "pipeline_id": pipeline_id,
            "name": data.get("spec", {}).get("name", "?"),
            "state": data.get("state", "?"),
            "continuous": data.get("spec", {}).get("continuous", False),
            "latest_updates": data.get("latest_updates", [])[:3],
        }


if __name__ == "__main__":
    mgr = LakebaseSyncManager(profile="cielo_demo")

    print("\n=== Online Catalogs ===")
    for c in mgr.list_online_catalogs():
        print(f"  {c['name']}")

    print("\n=== Existing Syncs ===")
    for s in mgr.list_syncs():
        mode = "continuous" if s["continuous"] else "triggered"
        print(f"  [{s['state']:8s}] {s['src_table']} -> {s['dest_table']} ({mode})")
