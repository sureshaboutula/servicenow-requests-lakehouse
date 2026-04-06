# Databricks notebook source
# src/bronze/autoloader_ingestion.py
# =======================================================
# Bronze Layer — Auto Loader Ingestion
# Reads Parquet files from S3 processed_requests folder
# Writes to Bronze Delta table in Unity Catalog
# Triggered when new files arrive in S3
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

print(f"""
=== Bronze Layer Configuration ===
Environment   : {env}
Catalog       : {catalog}
Bronze Table  : {bronze_table}
Raw S3 Path   : {raw_path}
Checkpoint    : {checkpoint_path}
==================================
""")

# -------------------------------------------------------
# Bronze Schema
# Enforce schema on raw data from S3
# -------------------------------------------------------
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType
)

bronze_schema = StructType([
    StructField("request_id",         StringType(),    nullable=True),
    StructField("orderType",          StringType(),    nullable=True),
    StructField("requested_by_name",  StringType(),    nullable=True),
    StructField("request_created_at", StringType(),    nullable=True),
    StructField("current_status",     StringType(),    nullable=True),
    StructField("last_updated_at",    TimestampType(), nullable=True),
    StructField("latest_comment",     StringType(),    nullable=True),
    StructField("owner_name",         StringType(),    nullable=True),
    StructField("owner_email",        StringType(),    nullable=True),
    StructField("location",           StringType(),    nullable=True),
    StructField("department",         StringType(),    nullable=True),
])

# -------------------------------------------------------
# Auto Loader — Read Stream from S3
# -------------------------------------------------------
print("Starting Auto Loader stream from S3...")

df_bronze = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("recursiveFileLookup", "true")
        .schema(bronze_schema)
        .load(raw_path)                               # S3 source path
)

# -------------------------------------------------------
# Add Metadata Columns
# -------------------------------------------------------
from pyspark.sql.functions import (
    current_timestamp,
    lit,
    to_timestamp,
    col
)

df_bronze = (
    df_bronze
        # Fix schema inconsistency — cast request_created_at to timestamp
        .withColumn("request_created_at",
            to_timestamp(col("request_created_at"), "yyyy-MM-dd HH:mm:ss"))
        # Fix last_updated_at inconsistency across partitions
        .withColumn("last_updated_at",
            to_timestamp(col("last_updated_at").cast("string"), "yyyy-MM-dd HH:mm:ss"))
        # Metadata columns
        .withColumn("_ingested_at",   current_timestamp())
        .withColumn("_source_file",   col("_metadata.file_path"))
        .withColumn("_env",           lit(env))
)

# -------------------------------------------------------
# Write Stream to Bronze Delta Table
# -------------------------------------------------------
print(f"Writing stream to Bronze table: {bronze_table}")

(
    df_bronze.writeStream
        .format("delta")                              # Write as Delta table
        .outputMode("append")                         # Append only for Bronze
        .option("checkpointLocation", checkpoint_path) # Track processed files
        .option("mergeSchema", "true")                # Handle schema evolution
        .trigger(availableNow=True)                   # Process all available files then stop
        .toTable(bronze_table)                        # Write to Unity Catalog table
        .awaitTermination()                           # Wait for stream to complete
)

print(f"✅ Bronze ingestion complete!")

# -------------------------------------------------------
# Verify — Show ingested records
# -------------------------------------------------------
print(f"\nVerifying Bronze table: {bronze_table}")
df_verify = spark.read.table(bronze_table)

print(f"Total Records : {df_verify.count()}")
print(f"Columns       : {df_verify.columns}")
print(f"\nSample Records:")
display(df_verify.limit(5))

print(f"""
╔══════════════════════════════════════════════╗
║         Bronze Ingestion Complete ✅         ║
║  Table    : {bronze_table:<32} ║
║  Records  : {str(df_verify.count()):<32} ║
╚══════════════════════════════════════════════╝
""")