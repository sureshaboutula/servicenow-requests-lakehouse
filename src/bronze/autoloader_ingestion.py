# Databricks notebook source
# src/bronze/autoloader_ingestion.py
# =======================================================
# Bronze Layer — Auto Loader Ingestion
# Reads Parquet files from S3 processed_requests folder
# Writes to Bronze Delta table as External Table on S3
# =======================================================

# -------------------------------------------------------
# Get variables from DAB widgets
# -------------------------------------------------------
env           = dbutils.widgets.get("env")
schema_bronze = dbutils.widgets.get("schema_bronze")

# -------------------------------------------------------
# Derived values
# -------------------------------------------------------
catalog      = f"servicenow_requests_{env}"
bronze_table = f"{catalog}.{schema_bronze}.raw_requests"

# S3 Paths
raw_path        = "s3://dynamodb-project-exports/processed_requests/"
checkpoint_path = f"s3://dynamodb-project-exports/checkpoints/{env}/bronze/"
bronze_path     = "s3://dynamodb-project-exports/db-bronze/raw_requests/"
schema_path     = f"s3://dynamodb-project-exports/checkpoints/{env}/bronze_schema/"

print(f"""
=== Bronze Layer Configuration ===
Environment   : {env}
Catalog       : {catalog}
Bronze Table  : {bronze_table}
Raw S3 Path   : {raw_path}
Bronze S3 Path: {bronze_path}
Checkpoint    : {checkpoint_path}
==================================
""")

# -------------------------------------------------------
# Imports
# -------------------------------------------------------
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, to_timestamp, current_timestamp, lit

bronze_schema = StructType([
    StructField("request_id",         StringType(), True),
    StructField("orderType",          StringType(), True),
    StructField("requested_by_name",  StringType(), True),
    StructField("request_created_at", StringType(), True),
    StructField("current_status",     StringType(), True),
    StructField("last_updated_at",    StringType(), True),  # Always string from Glue
    StructField("latest_comment",     StringType(), True),
    StructField("owner_name",         StringType(), True),
    StructField("owner_email",        StringType(), True),
    StructField("location",           StringType(), True),
    StructField("department",         StringType(), True),
])
# -------------------------------------------------------
# Auto Loader — Read
# -------------------------------------------------------
print("Starting Auto Loader stream from S3...")

df_bronze = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("recursiveFileLookup", "true")
        .option("cloudFiles.schemaLocation", schema_path)
        .schema(bronze_schema)
        .load(raw_path)
)

# -------------------------------------------------------
# Cast and Add Metadata Columns
# -------------------------------------------------------
df_bronze = (
    df_bronze
        .withColumn("request_created_at",
            to_timestamp(col("request_created_at"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("last_updated_at",
            to_timestamp(col("last_updated_at"), "yyyy-MM-dd HH:mm:ss"))
        .select(
            col("request_id"),
            col("orderType"),
            col("requested_by_name"),
            col("request_created_at"),
            col("current_status"),
            col("last_updated_at"),
            col("latest_comment"),
            col("owner_name"),
            col("owner_email"),
            col("location"),
            col("department"),
            current_timestamp().alias("_ingested_at"),
            col("_metadata.file_path").alias("_source_file"),
            lit(env).alias("_env")
        )
)

# -------------------------------------------------------
# Write Stream to Bronze Delta Table — External S3 Location
# -------------------------------------------------------
print(f"Writing stream to Bronze table: {bronze_table}")
print(f"External S3 location: {bronze_path}")

(
    df_bronze.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
        .option("path", bronze_path)              # ← External S3 location
        .option("mergeSchema", "true")
        .trigger(availableNow=True)
        .toTable(bronze_table)
        .awaitTermination()
)

print(f"✅ Bronze ingestion complete!")

# -------------------------------------------------------
# Verify
# -------------------------------------------------------
print(f"\nVerifying Bronze table: {bronze_table}")
df_verify = spark.read.table(bronze_table)

print(f"Total Records        : {df_verify.count()}")
print(f"Null last_updated_at : {df_verify.filter(col('last_updated_at').isNull()).count()}")
print(f"\nSample Records:")
display(df_verify.limit(5))

print(f"""
╔══════════════════════════════════════════════╗
║         Bronze Ingestion Complete ✅         ║
║  Table  : {bronze_table:<34} ║
║  S3     : {bronze_path:<34} ║
║  Records: {str(df_verify.count()):<34} ║
╚══════════════════════════════════════════════╝
""")