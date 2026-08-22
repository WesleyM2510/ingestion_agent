"""
Generates a synthetic Delta table of a target size.
Pass TABLE_NAME, TARGET_ROWS, NUM_PARTITIONS as task parameters or hardcode below.
Uses pure Spark functions - no external dependencies.
"""
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

spark = SparkSession.builder.getOrCreate()

# Read params from task parameters
table_name = sys.argv[1]
target_rows = int(sys.argv[2])
num_partitions = int(sys.argv[3])

catalog_schema = "cielo_demo.default"
full_table = f"{catalog_schema}.{table_name}"

print(f"=== Generating {full_table} with {target_rows:,} rows, {num_partitions} partitions ===")

df = (
    spark.range(0, target_rows, 1, num_partitions)
    .withColumn("pk", F.expr("uuid()"))
    .withColumn("customer_name", F.concat(
        F.expr("substring(md5(cast(id as string)), 1, 8)"),
        F.lit(" "),
        F.expr("substring(md5(cast(id + 1 as string)), 1, 8)")
    ))
    .withColumn("email", F.concat(
        F.expr("substring(md5(cast(id * 7 as string)), 1, 10)"),
        F.lit("@"),
        F.expr("substring(md5(cast(id * 13 as string)), 1, 6)"),
        F.lit(".com")
    ))
    .withColumn("description", F.concat(
        F.expr("md5(cast(id * 3 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 5 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 9 as string))")
    ))
    .withColumn("amount", F.round(F.rand(42) * 99999 + 0.01, 2))
    .withColumn("quantity", (F.rand(43) * 9999 + 1).cast("int"))
    .withColumn("unit_price", F.round(F.rand(44) * 9999 + 0.01, 2))
    .withColumn("discount", F.round(F.rand(45) * 99, 2))
    .withColumn("tax_rate", F.round(F.rand(46) * 25, 2))
    .withColumn("category", F.element_at(
        F.array(*[F.lit(c) for c in ["Electronics", "Food", "Clothing", "Travel", "Health", "Finance", "Education", "Entertainment"]]),
        (F.rand(47) * 8).cast("int") + 1
    ))
    .withColumn("status", F.element_at(
        F.array(*[F.lit(s) for s in ["Active", "Pending", "Completed", "Cancelled", "Failed"]]),
        (F.rand(48) * 5).cast("int") + 1
    ))
    .withColumn("region", F.element_at(
        F.array(*[F.lit(r) for r in ["US-East", "US-West", "EU-West", "EU-Central", "APAC", "LATAM"]]),
        (F.rand(49) * 6).cast("int") + 1
    ))
    .withColumn("channel", F.element_at(
        F.array(*[F.lit(c) for c in ["Online", "Mobile", "In-Store", "Partner"]]),
        (F.rand(50) * 4).cast("int") + 1
    ))
    .withColumn("created_date", F.date_add(F.lit("2020-01-01"), (F.rand(51) * 2323).cast("int")))
    .withColumn("updated_ts", F.to_timestamp(
        F.date_add(F.lit("2020-01-01"), (F.rand(52) * 2323).cast("int"))
    ))
    .withColumn("is_flagged", F.rand(53) > 0.5)
    .withColumn("score", F.round(F.rand(54) * 100, 4))
    .withColumn("notes", F.concat(
        F.expr("md5(cast(id * 11 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 17 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 23 as string))"),
        F.lit(" "),
        F.expr("md5(cast(id * 29 as string))")
    ))
    .withColumn("payload", F.expr("md5(cast(id * 31 as string))"))
    .drop("id")
)

print(f"Writing {full_table}...")
df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_table)

detail = spark.sql(f"DESCRIBE DETAIL {full_table}").collect()[0]
size_bytes = detail["sizeInBytes"]
num_files = detail["numFiles"]
row_count = spark.table(full_table).count()
print(f"DONE: {full_table} | Rows: {row_count:,} | Size: {size_bytes / (1024**2):,.0f} MB ({size_bytes / (1024**3):.2f} GB) | Files: {num_files}")
