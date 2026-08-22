"""
MERGE additional rows into existing synthetic tables to hit target sizes.
Uses the actual bytes/row rate from the existing data to calculate exact row counts.
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.getOrCreate()

table_name = sys.argv[1]
additional_rows = int(sys.argv[2])
num_partitions = int(sys.argv[3])

catalog_schema = "cielo_demo.default"
full_table = f"{catalog_schema}.{table_name}"

print(f"=== MERGE into {full_table}: adding {additional_rows:,} rows ({num_partitions} partitions) ===")

# Generate source DataFrame with new rows (new UUIDs = no matches = all inserts)
source = (
    spark.range(0, additional_rows, 1, num_partitions)
    .withColumn("pk", F.expr("uuid()"))
    .withColumn("customer_name", F.concat(
        F.expr("substring(md5(cast(id + 999999999 as string)), 1, 8)"),
        F.lit(" "),
        F.expr("substring(md5(cast(id + 888888888 as string)), 1, 8)")
    ))
    .withColumn("email", F.concat(
        F.expr("substring(md5(cast(id * 71 as string)), 1, 10)"),
        F.lit("@"),
        F.expr("substring(md5(cast(id * 131 as string)), 1, 6)"),
        F.lit(".com")
    ))
    .withColumn("description", F.concat(
        F.expr("md5(cast(id * 37 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 53 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 97 as string))")
    ))
    .withColumn("amount", F.round(F.rand(142) * 99999 + 0.01, 2))
    .withColumn("quantity", (F.rand(143) * 9999 + 1).cast("int"))
    .withColumn("unit_price", F.round(F.rand(144) * 9999 + 0.01, 2))
    .withColumn("discount", F.round(F.rand(145) * 99, 2))
    .withColumn("tax_rate", F.round(F.rand(146) * 25, 2))
    .withColumn("category", F.element_at(
        F.array(*[F.lit(c) for c in ["Electronics", "Food", "Clothing", "Travel", "Health", "Finance", "Education", "Entertainment"]]),
        (F.rand(147) * 8).cast("int") + 1
    ))
    .withColumn("status", F.element_at(
        F.array(*[F.lit(s) for s in ["Active", "Pending", "Completed", "Cancelled", "Failed"]]),
        (F.rand(148) * 5).cast("int") + 1
    ))
    .withColumn("region", F.element_at(
        F.array(*[F.lit(r) for r in ["US-East", "US-West", "EU-West", "EU-Central", "APAC", "LATAM"]]),
        (F.rand(149) * 6).cast("int") + 1
    ))
    .withColumn("channel", F.element_at(
        F.array(*[F.lit(c) for c in ["Online", "Mobile", "In-Store", "Partner"]]),
        (F.rand(150) * 4).cast("int") + 1
    ))
    .withColumn("created_date", F.date_add(F.lit("2020-01-01"), (F.rand(151) * 2323).cast("int")))
    .withColumn("updated_ts", F.to_timestamp(
        F.date_add(F.lit("2020-01-01"), (F.rand(152) * 2323).cast("int"))
    ))
    .withColumn("is_flagged", F.rand(153) > 0.5)
    .withColumn("score", F.round(F.rand(154) * 100, 4))
    .withColumn("notes", F.concat(
        F.expr("md5(cast(id * 113 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 173 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 233 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 293 as string))")
    ))
    .withColumn("payload", F.expr("md5(cast(id * 313 as string))"))
    .drop("id")
)

source.createOrReplaceTempView("source_data")

print("Running MERGE...")
spark.sql(f"""
    MERGE INTO {full_table} AS target
    USING source_data AS source
    ON target.pk = source.pk
    WHEN NOT MATCHED THEN INSERT *
""")

detail = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]
size_bytes = detail["sizeInBytes"]
num_files = detail["numFiles"]
print(f"DONE: {full_table} | Size: {size_bytes / (1024**2):,.0f} MB ({size_bytes / (1024**3):.2f} GB) | Files: {num_files}")
