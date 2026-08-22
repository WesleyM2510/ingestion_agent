"""
Upsert 5% of rows into each synthetic table.
- 50% of upserted rows are UPDATEs (existing PKs, modified values)
- 50% are INSERTs (new PKs)
This simulates a realistic CDC workload for Lakebase sync testing.
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.getOrCreate()

table_name = sys.argv[1]
full_table = f"cielo_demo.default.{table_name}"

# Get current row count
current_count = spark.table(full_table).count()
upsert_count = int(current_count * 0.05)
update_count = upsert_count // 2
insert_count = upsert_count - update_count

print(f"=== Upsert {full_table} ===")
print(f"  Current rows:  {current_count:,}")
print(f"  Total upsert:  {upsert_count:,} (5%)")
print(f"  Updates:       {update_count:,}")
print(f"  Inserts:       {insert_count:,}")

# Get existing PKs for updates (sample 50% of upsert count)
existing_pks = (
    spark.table(full_table)
    .select("pk")
    .orderBy(F.rand(seed=99))
    .limit(update_count)
)

# Build UPDATE rows: take existing PKs, change other columns
updates = (
    existing_pks
    .withColumn("customer_name", F.concat(
        F.expr("substring(md5(pk), 1, 8)"),
        F.lit(" UPDATED")
    ))
    .withColumn("email", F.concat(
        F.expr("substring(md5(pk), 1, 10)"),
        F.lit("@updated.com")
    ))
    .withColumn("description", F.concat(
        F.lit("UPSERTED "),
        F.expr("md5(pk)"),
        F.lit(" "),
        F.expr("md5(concat(pk, 'v2'))")
    ))
    .withColumn("amount", F.round(F.rand(200) * 99999 + 0.01, 2))
    .withColumn("quantity", (F.rand(201) * 9999 + 1).cast("int"))
    .withColumn("unit_price", F.round(F.rand(202) * 9999 + 0.01, 2))
    .withColumn("discount", F.round(F.rand(203) * 99, 2))
    .withColumn("tax_rate", F.round(F.rand(204) * 25, 2))
    .withColumn("category", F.element_at(
        F.array(*[F.lit(c) for c in ["Electronics", "Food", "Clothing", "Travel", "Health", "Finance", "Education", "Entertainment"]]),
        (F.rand(205) * 8).cast("int") + 1
    ))
    .withColumn("status", F.lit("Updated"))
    .withColumn("region", F.element_at(
        F.array(*[F.lit(r) for r in ["US-East", "US-West", "EU-West", "EU-Central", "APAC", "LATAM"]]),
        (F.rand(206) * 6).cast("int") + 1
    ))
    .withColumn("channel", F.element_at(
        F.array(*[F.lit(c) for c in ["Online", "Mobile", "In-Store", "Partner"]]),
        (F.rand(207) * 4).cast("int") + 1
    ))
    .withColumn("created_date", F.date_add(F.lit("2020-01-01"), (F.rand(208) * 2323).cast("int")))
    .withColumn("updated_ts", F.current_timestamp())
    .withColumn("is_flagged", F.lit(True))
    .withColumn("score", F.round(F.rand(209) * 100, 4))
    .withColumn("notes", F.concat(F.lit("UPSERT_BATCH "), F.expr("md5(pk)"), F.lit(" "), F.expr("md5(concat(pk,'x'))")))
    .withColumn("payload", F.expr("md5(concat(pk, 'upserted'))"))
)

# Build INSERT rows: new UUIDs
inserts = (
    spark.range(0, insert_count, 1, max(1, insert_count // 100000))
    .withColumn("pk", F.expr("uuid()"))
    .withColumn("customer_name", F.concat(
        F.expr("substring(md5(cast(id + 77777777 as string)), 1, 8)"),
        F.lit(" NEW")
    ))
    .withColumn("email", F.concat(
        F.expr("substring(md5(cast(id * 71 as string)), 1, 10)"),
        F.lit("@new.com")
    ))
    .withColumn("description", F.concat(
        F.lit("NEW_INSERT "),
        F.expr("md5(cast(id * 37 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 53 as string))")
    ))
    .withColumn("amount", F.round(F.rand(210) * 99999 + 0.01, 2))
    .withColumn("quantity", (F.rand(211) * 9999 + 1).cast("int"))
    .withColumn("unit_price", F.round(F.rand(212) * 9999 + 0.01, 2))
    .withColumn("discount", F.round(F.rand(213) * 99, 2))
    .withColumn("tax_rate", F.round(F.rand(214) * 25, 2))
    .withColumn("category", F.element_at(
        F.array(*[F.lit(c) for c in ["Electronics", "Food", "Clothing", "Travel", "Health", "Finance", "Education", "Entertainment"]]),
        (F.rand(215) * 8).cast("int") + 1
    ))
    .withColumn("status", F.lit("New"))
    .withColumn("region", F.element_at(
        F.array(*[F.lit(r) for r in ["US-East", "US-West", "EU-West", "EU-Central", "APAC", "LATAM"]]),
        (F.rand(216) * 6).cast("int") + 1
    ))
    .withColumn("channel", F.element_at(
        F.array(*[F.lit(c) for c in ["Online", "Mobile", "In-Store", "Partner"]]),
        (F.rand(217) * 4).cast("int") + 1
    ))
    .withColumn("created_date", F.date_add(F.lit("2020-01-01"), (F.rand(218) * 2323).cast("int")))
    .withColumn("updated_ts", F.current_timestamp())
    .withColumn("is_flagged", F.lit(False))
    .withColumn("score", F.round(F.rand(219) * 100, 4))
    .withColumn("notes", F.concat(F.lit("NEW_BATCH "), F.expr("md5(cast(id * 113 as string))"), F.lit(" "), F.expr("md5(cast(id * 173 as string))")))
    .withColumn("payload", F.expr("md5(cast(id * 313 as string))"))
    .drop("id")
)

# Combine into source for MERGE
source = updates.unionByName(inserts)
source.createOrReplaceTempView("upsert_source")

print(f"Running MERGE ({source.count():,} rows)...")
spark.sql(f"""
    MERGE INTO {full_table} AS target
    USING upsert_source AS source
    ON target.pk = source.pk
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

final_count = spark.table(full_table).count()
print(f"DONE: {full_table} | Before: {current_count:,} | After: {final_count:,} | Delta: +{final_count - current_count:,}")
