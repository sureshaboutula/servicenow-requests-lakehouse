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

df_scd = (
    df_cleaned
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
            .option("path", silver_path)          # ← External S3 location
            .saveAsTable(silver_table)
    )
    print(f"✅ Silver table created with {df_scd.count()} records")

else:
    print(f"Silver table found. Applying SCD Type 2 merge...")
    delta_table = DeltaTable.forName(spark, silver_table)

    # Expire changed records
    delta_table.update(
        condition = f"""
            is_current = true
            AND EXISTS (
                SELECT 1 FROM {bronze_table} src
                WHERE src.request_id = {silver_table}.request_id
                AND src.current_status != {silver_table}.current_status
            )
        """,
        set = {
            "is_current"        : "false",
            "effective_end_date": "current_timestamp()"
        }
    )

    # Insert new/changed records
    (
        delta_table.alias("silver")
            .merge(
                df_scd.alias("bronze"),
                """
                silver.request_id = bronze.request_id
                AND silver.current_status = bronze.current_status
                AND silver.is_current = true
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
    )
    print(f"✅ SCD Type 2 merge complete")

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