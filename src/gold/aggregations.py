# Databricks notebook source
# src/gold/aggregations.py
# =======================================================
# Gold Layer — Aggregations and Business Ready Tables
# Reads from Silver Delta table (current records only)
# Creates aggregated tables for reporting
# =======================================================

# -------------------------------------------------------
# Get variables from DAB widgets
# -------------------------------------------------------
env           = dbutils.widgets.get("env")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")

# -------------------------------------------------------
# Derived values
# -------------------------------------------------------
catalog               = f"servicenow_requests_{env}"
silver_table          = f"{catalog}.{schema_silver}.requests_scd"
gold_by_status        = f"{catalog}.{schema_gold}.requests_by_status"
gold_by_department    = f"{catalog}.{schema_gold}.requests_by_department"
gold_by_location      = f"{catalog}.{schema_gold}.requests_by_location"
gold_owner_workload   = f"{catalog}.{schema_gold}.owner_workload"

print(f"""
=== Gold Layer Configuration ===
Environment   : {env}
Catalog       : {catalog}
Silver Table  : {silver_table}
Gold Tables:
  - {gold_by_status}
  - {gold_by_department}
  - {gold_by_location}
  - {gold_owner_workload}
=================================
""")

# -------------------------------------------------------
# Imports
# -------------------------------------------------------
from pyspark.sql.functions import (
    col, count, countDistinct,
    current_timestamp, lit,
    round, avg, max, min,
    date_trunc, datediff, when
)

# -------------------------------------------------------
# Step 1 — Read Current Records from Silver
# -------------------------------------------------------
print("Step 1: Reading current records from Silver...")

df_silver = (
    spark.read
        .table(silver_table)
        .filter(col("is_current") == True)
)

print(f"✅ Current Silver records: {df_silver.count()}")

# Cache for multiple aggregations
#df_silver.cache()

# -------------------------------------------------------
# Step 2 — Requests by Status
# -------------------------------------------------------
print("\nStep 2: Creating requests_by_status...")

df_by_status = (
    df_silver
        .groupBy("current_status")
        .agg(
            count("request_id").alias("total_requests"),
            countDistinct("request_id").alias("unique_requests"),
            countDistinct("department").alias("departments_affected"),
            current_timestamp().alias("_updated_at")
        )
        .orderBy("total_requests", ascending=False)
)

(
    df_by_status.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_by_status)
)
print(f"✅ {gold_by_status} created")
display(df_by_status)

# -------------------------------------------------------
# Step 3 — Requests by Department
# -------------------------------------------------------
print("\nStep 3: Creating requests_by_department...")

df_by_department = (
    df_silver
        .groupBy("department", "current_status")
        .agg(
            count("request_id").alias("total_requests"),
            countDistinct("order_type").alias("order_types_count"),
            current_timestamp().alias("_updated_at")
        )
        .orderBy("department", "total_requests", ascending=False)
)

(
    df_by_department.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_by_department)
)
print(f"✅ {gold_by_department} created")
display(df_by_department)

# -------------------------------------------------------
# Step 4 — Requests by Location
# -------------------------------------------------------
print("\nStep 4: Creating requests_by_location...")

df_by_location = (
    df_silver
        .groupBy("location", "current_status")
        .agg(
            count("request_id").alias("total_requests"),
            countDistinct("department").alias("departments_count"),
            countDistinct("order_type").alias("order_types_count"),
            current_timestamp().alias("_updated_at")
        )
        .orderBy("location", "total_requests", ascending=False)
)

(
    df_by_location.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_by_location)
)
print(f"✅ {gold_by_location} created")
display(df_by_location)

# -------------------------------------------------------
# Step 5 — Owner Workload
# -------------------------------------------------------
print("\nStep 5: Creating owner_workload...")

df_owner_workload = (
    df_silver
        .groupBy("owner_name", "owner_email", "department")
        .agg(
            count("request_id").alias("total_requests"),
            count(
                when(col("current_status") == "PENDING", 1)
            ).alias("pending_requests"),
            count(
                when(col("current_status") == "COMPLETED", 1)
            ).alias("completed_requests"),
            count(
                when(col("current_status") == "FAILED", 1)
            ).alias("failed_requests"),
            current_timestamp().alias("_updated_at")
        )
        .orderBy("total_requests", ascending=False)
)

(
    df_owner_workload.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(gold_owner_workload)
)
print(f"✅ {gold_owner_workload} created")
display(df_owner_workload)

# -------------------------------------------------------
# Step 6 — Unpersist Cache
# -------------------------------------------------------
#df_silver.unpersist()

# -------------------------------------------------------
# Step 7 — Final Summary
# -------------------------------------------------------
print(f"""
╔══════════════════════════════════════════════════════╗
║           Gold Layer Complete ✅                     ║
╠══════════════════════════════════════════════════════╣
║  ✅ requests_by_status                               ║
║  ✅ requests_by_department                           ║
║  ✅ requests_by_location                             ║
║  ✅ owner_workload                                   ║
╠══════════════════════════════════════════════════════╣
║  All tables in: {catalog}.{schema_gold:<28}  ║
╚══════════════════════════════════════════════════════╝
""")