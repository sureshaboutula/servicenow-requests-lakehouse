# Databricks notebook source
# src/silver/cleaning.py
# =======================================================
# Silver Layer — Cleaning and Transformations
# Reads from Bronze Delta table
# Writes to Silver Delta table as External Table on S3
# =======================================================

# -------------------------------------------------------
# Get variables from DAB widgets
# -------------------------------------------------------
env           = dbutils.widgets.get("env")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")

# -------------------------------------------------------
# Derived values
# -------------------------------------------------------
catalog       = f"servicenow_requests_{env}"
bronze_table  = f"{catalog}.{schema_bronze}.raw_requests"
silver_table  = f"{catalog}.{schema_silver}.requests_scd"
silver_path   = "s3://dynamodb-project-exports/db-silver/requests_scd/"

print(f"""
=== Silver Layer Configuration ===
Environment   : {env}
Catalog       : {catalog}
Bronze Table  : {bronze_table}
Silver Table  : {silver_table}
Silver S3 Path: {silver_path}
===================================
""")

# -------------------------------------------------------
# Imports
# -------------------------------------------------------
from pyspark.sql.functions import (
    col, trim, lower, upper,
    to_timestamp, current_timestamp,
    when, lit, coalesce
)
from delta.tables import DeltaTable

# -------------------------------------------------------
# Step 1 — Read from Bronze
# -------------------------------------------------------
print("Step 1: Reading from Bronze table...")
df_bronze = spark.read.table(bronze_table)
print(f"✅ Bronze records read: {df_bronze.count()}")

# -------------------------------------------------------
# Step 2 — Clean and Standardize
# -------------------------------------------------------
print("\nStep 2: Cleaning and standardizing data...")

df_cleaned = (
    df_bronze
        .dropDuplicates(["request_id", "last_updated_at"])
        .filter(col("request_id").isNotNull())
        .withColumn("request_id",        trim(col("request_id")))
        .withColumn("requested_by_name", trim(col("requested_by_name")))
        .withColumn("owner_name",        trim(col("owner_name")))
        .withColumn("owner_email",       lower(trim(col("owner_email"))))
        .withColumn("location",          trim(col("location")))
        .withColumn("department",        trim(col("department")))
        .withColumnRenamed("orderType",  "order_type")
        .withColumn("current_status",
            upper(trim(col("current_status"))))
        .withColumn("latest_comment",
            trim(coalesce(col("latest_comment"), lit("No comment"))))
        .drop("_source_file", "_env")
        .withColumn("_cleaned_at", current_timestamp())
)

print(f"✅ Records after cleaning: {df_cleaned.count()}")

# -------------------------------------------------------
# Step 3 — Apply SCD Type 2
# -------------------------------------------------------
print("\nStep 3: Applying SCD Type 2...")

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, current_timestamp, row_number
from pyspark.sql.window import Window

# -------------------------------------------------------
# Get LATEST record per request_id from Bronze
# This avoids comparing against old bronze records
# -------------------------------------------------------
window_spec = Window.partitionBy("request_id").orderBy(col("last_updated_at").desc())

df_latest_bronze = (
    df_cleaned
        .withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
)

print(f"Latest bronze records: {df_latest_bronze.count()}")

# Add SCD Type 2 columns
df_scd = (
    df_latest_bronze
        .withColumn("effective_start_date", col("last_updated_at"))
        .withColumn("effective_end_date",   lit(None).cast("timestamp"))
        .withColumn("is_current",           lit(True))
)

table_exists = spark.catalog.tableExists(silver_table)

if not table_exists:
    print(f"Silver table not found. Creating: {silver_table}")
    (
        df_scd.write
            .format("delta")
            .mode("overwrite")
            .option("path", silver_path)
            .saveAsTable(silver_table)
    )
    print(f"✅ Silver table created with {df_scd.count()} records")

else:
    print(f"Silver table found. Applying SCD Type 2 merge...")
    delta_table = DeltaTable.forName(spark, silver_table)

    # -------------------------------------------------------
    # Step 3a — Expire ONLY records where status changed
    # Compare silver current records against LATEST bronze
    # -------------------------------------------------------
    delta_table.alias("silver").merge(
        df_scd.alias("latest"),
        "silver.request_id = latest.request_id AND silver.is_current = true"
    ).whenMatchedUpdate(
        condition = "silver.current_status != latest.current_status",
        set = {
            "is_current"        : "false",
            "effective_end_date": "current_timestamp()"
        }
    ).execute()

    print(f"✅ Expired changed records")

    # -------------------------------------------------------
    # Step 3b — Insert new records and changed records
    # Only insert if not already current with same status
    # -------------------------------------------------------
    (
        delta_table.alias("silver")
            .merge(
                df_scd.alias("new"),
                """
                silver.request_id = new.request_id
                AND silver.current_status = new.current_status
                AND silver.is_current = true
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
    )
    print(f"✅ Inserted new/changed records")

# -------------------------------------------------------
# Step 4 — Verify
# -------------------------------------------------------
print(f"\nStep 4: Verifying Silver table...")
df_verify = spark.read.table(silver_table)

print(f"Total Records  : {df_verify.count()}")
print(f"Current Records: {df_verify.filter(col('is_current') == True).count()}")
print(f"History Records: {df_verify.filter(col('is_current') == False).count()}")

display(
    df_verify
        .filter(col("is_current") == True)
        .groupBy("current_status")
        .count()
        .orderBy("count", ascending=False)
)

print(f"""
╔══════════════════════════════════════════════╗
║       Silver Layer Complete ✅               ║
║  Table : {silver_table:<34} ║
║  S3    : {silver_path:<34} ║
╚══════════════════════════════════════════════╝
""")