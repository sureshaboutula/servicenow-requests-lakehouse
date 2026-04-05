# Databricks notebook source
# src/silver/scd_type2.py
# =======================================================
# SCD Type 2 Helper — Reusable Functions
# Used by cleaning.py for status change tracking
# =======================================================

from delta.tables import DeltaTable
from pyspark.sql.functions import lit, current_timestamp
from pyspark.sql import DataFrame

def apply_scd_type2(
    spark,
    df_new: DataFrame,
    silver_table: str,
    merge_key: str,
    tracked_column: str
):
    """
    Applies SCD Type 2 logic to Silver Delta table.

    Args:
        spark        : SparkSession
        df_new       : Incoming cleaned DataFrame
        silver_table : Full table name (catalog.schema.table)
        merge_key    : Column to match records (e.g. request_id)
        tracked_column: Column to track changes (e.g. current_status)
    """

    table_exists = spark.catalog.tableExists(silver_table)

    if not table_exists:
        # First run — create table
        print(f"Creating Silver table: {silver_table}")
        (
            df_new.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(silver_table)
        )
        print(f"✅ Silver table created")
        return

    # Get Delta table reference
    delta_table = DeltaTable.forName(spark, silver_table)

    # Step 1 — Expire changed records
    delta_table.update(
        condition = f"""
            is_current = true
            AND EXISTS (
                SELECT 1 FROM (SELECT * FROM {silver_table}) src
                WHERE src.{merge_key} = {silver_table}.{merge_key}
                AND src.{tracked_column} != {silver_table}.{tracked_column}
            )
        """,
        set = {
            "is_current"        : "false",
            "effective_end_date": "current_timestamp()"
        }
    )

    # Step 2 — Insert new/changed records
    (
        delta_table.alias("silver")
            .merge(
                df_new.alias("new"),
                f"""
                silver.{merge_key} = new.{merge_key}
                AND silver.{tracked_column} = new.{tracked_column}
                AND silver.is_current = true
                """
            )
            .whenNotMatchedInsertAll()
            .execute()
    )

    print(f"✅ SCD Type 2 merge complete for {silver_table}")