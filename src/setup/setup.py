# Databricks notebook source
# src/setup/setup.py
# =======================================================
# Setup Script — Creates Catalog and Schemas
# Runs as first task in Databricks Workflow
# All variables passed via DAB → Databricks Widgets
# =======================================================

# -------------------------------------------------------
# Get variables from DAB widgets
# -------------------------------------------------------
env           = dbutils.widgets.get("env")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold   = dbutils.widgets.get("schema_gold")

# -------------------------------------------------------
# Derived values
# -------------------------------------------------------
catalog = f"servicenow_requests_{env}"
schemas = [schema_bronze, schema_silver, schema_gold]

print(f"""
=== Setup Configuration ===
Environment : {env}
Catalog     : {catalog}
Schemas     : {schemas}
===========================
""")

# -------------------------------------------------------
# Create Catalog
# -------------------------------------------------------
def create_catalog(catalog):
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {catalog}
        COMMENT 'ServiceNow Requests Lakehouse - {env} environment'
    """)
    print(f"✅ Catalog '{catalog}' ready")

create_catalog(catalog)

# -------------------------------------------------------
# Create Schemas
# -------------------------------------------------------
def create_schema(catalog, schema_name):
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {schema_name}
        COMMENT '{schema_name} layer - {env} environment'
    """)
    print(f"✅ Schema '{catalog}.{schema_name}' ready")

for schema in schemas:
    create_schema(catalog, schema)

# -------------------------------------------------------
# Verify
# -------------------------------------------------------
print(f"\nVerifying setup for catalog '{catalog}':")
result = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
for row in result:
    print(f"   └── {catalog}.{row['databaseName']}")

print("\n✅ Setup complete!")