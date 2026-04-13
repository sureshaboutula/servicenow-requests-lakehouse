# Databricks notebook source
# src/setup/setup.py
# =======================================================
# Setup Script — Creates Catalog, Schemas and External Locations
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
catalog            = f"servicenow_requests_{env}"
schemas            = [schema_bronze, schema_silver, schema_gold]
storage_credential = "s3-dynamodb-exports-credential"
checkpoint_path    = f"s3://dynamodb-project-exports/checkpoints/{env}/"

# S3 paths — env specific
raw_path        = "s3://dynamodb-project-exports/processed_requests/"
bronze_path     = f"s3://dynamodb-project-exports/db-bronze/{env}/"
silver_path     = f"s3://dynamodb-project-exports/db-silver/{env}/"
gold_path       = f"s3://dynamodb-project-exports/db-gold/{env}/"
checkpoint_path = f"s3://dynamodb-project-exports/checkpoints/{env}/"

print(f"""
=== Setup Configuration ===
Environment     : {env}
Catalog         : {catalog}
Schemas         : {schemas}
Bronze Path     : {bronze_path}
Silver Path     : {silver_path}
Gold Path       : {gold_path}
Checkpoint Path : {checkpoint_path}
===========================
""")

# -------------------------------------------------------
# Step 1 — Create Catalog
# -------------------------------------------------------
def create_catalog(catalog):
    spark.sql(f"""
        CREATE CATALOG IF NOT EXISTS {catalog}
        COMMENT 'ServiceNow Requests Lakehouse - {env} environment'
    """)
    print(f"✅ Catalog '{catalog}' ready")

create_catalog(catalog)

# -------------------------------------------------------
# Step 2 — Create Schemas
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
# Step 3 — Create External Locations (env specific)
# -------------------------------------------------------
print(f"\nStep 3: Creating external locations for {env}...")

external_locations = {
    f"el-raw"              : raw_path,        # shared — same for dev and prod
    f"el-bronze-{env}"     : bronze_path,
    f"el-silver-{env}"     : silver_path,
    f"el-gold-{env}"       : gold_path,
    f"el-checkpoint-{env}" : checkpoint_path,
}

for location_name, location_path in external_locations.items():
    try:
        spark.sql(f"""
            CREATE EXTERNAL LOCATION IF NOT EXISTS `{location_name}`
            URL '{location_path}'
            WITH (STORAGE CREDENTIAL `{storage_credential}`)
            COMMENT '{location_name} - {env} environment'
        """)
        print(f"✅ External location '{location_name}' → {location_path}")
    except Exception as e:
        if "LOCATION_OVERLAP" in str(e) or "already exists" in str(e).lower():
            print(f"⚠️  '{location_name}' already exists — skipping")
        else:
            print(f"❌ Failed '{location_name}': {str(e)[:200]}")

# -------------------------------------------------------
# Step 4 — Verify
# -------------------------------------------------------
print(f"\nStep 4: Verifying setup...")

print(f"\nSchemas in '{catalog}':")
result = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
for row in result:
    print(f"   └── {catalog}.{row['databaseName']}")

print(f"\nExternal Locations:")
display(spark.sql("SHOW EXTERNAL LOCATIONS"))

print("\n✅ Setup complete!")