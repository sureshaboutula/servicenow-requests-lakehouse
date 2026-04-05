# Databricks notebook source
# src/silver/cleaning.py
# =======================================================
# Silver Layer — Cleaning and Transformations
# Reads from Bronze Delta table
# Applies cleaning, standardization and SCD Type 2
# Writes to Silver Delta table
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

print(f"""
=== Silver Layer Configuration ===
Environment   : {env}
Catalog       : {catalog}
Bronze Table  : {bronze_table}
Silver Table  : {silver_table}
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
        # Drop duplicates based on request_id and last_updated_at
        .dropDuplicates(["request_id", "last_updated_at"])

        # Drop records with no request_id
        .filter(col("request_id").isNotNull())

        # Standardize string columns — trim whitespace
        .withColumn("request_id",        trim(col("request_id")))
        .withColumn("requested_by_name", trim(col("requested_by_name")))
        .withColumn("owner_name",        trim(col("owner_name")))
        .withColumn("owner_email",       lower(trim(col("owner_email"))))
        .withColumn("location",          trim(col("location")))
        .withColumn("department",        trim(col("department")))

        # Standardize order_type — rename camelCase to snake_case
        .withColumnRenamed("orderType", "order_type")

        # Standardize status — uppercase
        .withColumn("current_status",
            upper(trim(col("current_status"))))

        # Standardize latest_comment — trim
        .withColumn("latest_comment",
            trim(coalesce(col("latest_comment"), lit("No comment"))))

        # Drop metadata columns from Bronze — not needed in Silver
        .drop("_source_file", "_env")

        # Add Silver metadata
        .withColumn("_cleaned_at", current_timestamp())
)

print(f"✅ Records after cleaning: {df_cleaned.count()}")

# -------------------------------------------------------
# Step 3 — Apply SCD Type 2
# -------------------------------------------------------
print("\nStep 3: Applying SCD Type 2...")

from delta.tables import DeltaTable

# Add SCD Type 2 columns to incoming data
df_scd = (
    df_cleaned
        .withColumn("effective_start_date", col("last_updated_at"))
        .withColumn("effective_end_date",   lit(None).cast("timestamp"))
        .withColumn("is_current",           lit(True))
)

# Check if Silver table exists
table_exists = spark.catalog.tableExists(silver_table)

if not table_exists:
    # First run — write all records directly
    print(f"Silver table not found. Creating: {silver_table}")
    (
        df_scd.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
    )
    print(f"✅ Silver table created with {df_scd.count()} records")

else:
    # Subsequent runs — apply SCD Type 2 merge
    print(f"Silver table found. Applying SCD Type 2 merge...")
    delta_table = DeltaTable.forName(spark, silver_table)

    # Step 3a — Expire old records where status has changed
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
            "is_current"       : "false",
            "effective_end_date": "current_timestamp()"
        }
    )

    # Step 3b — Insert new/changed records
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
print(f"\nStatus Distribution:")
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
║  Table   : {silver_table:<33} ║
║  Total   : {str(df_verify.count()):<33} ║
╚══════════════════════════════════════════════╝
""")