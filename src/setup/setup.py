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

print(f"""
=== Setup Configuration ===
Environment     : {env}
Catalog         : {catalog}
Schemas         : {schemas}
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
# Step 3 — Create Checkpoint External Location
# -------------------------------------------------------
print(f"\nStep 3: Creating checkpoint external location...")

spark.sql(f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `el-checkpoint-{env}`
    URL '{checkpoint_path}'
    WITH (STORAGE CREDENTIAL `{storage_credential}`)
    COMMENT 'Auto Loader checkpoint location - {env} environment'
""")
print(f"✅ External location 'el-checkpoint-{env}' ready")

# -------------------------------------------------------
# Step 4 — Create Bronze External Location
# -------------------------------------------------------
print(f"\nStep 4: Creating bronze external location...")

spark.sql(f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `el-bronze-{env}`
    URL 's3://dynamodb-project-exports/db-bronze/{env}/'
    WITH (STORAGE CREDENTIAL `s3-dynamodb-exports-credential`)
    COMMENT 'Bronze layer external location - {env} environment'
""")
print(f"✅ External location 'el-bronze-{env}' ready")

# -------------------------------------------------------
# Step 5 — Create Silver External Location
# -------------------------------------------------------
print(f"\nStep 5: Creating silver external location...")

spark.sql(f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `el-silver-{env}`
    URL 's3://dynamodb-project-exports/db-silver/{env}/'
    WITH (STORAGE CREDENTIAL `s3-dynamodb-exports-credential`)
    COMMENT 'Silver layer external location - {env} environment'
""")
print(f"✅ External location 'el-silver-{env}' ready")

# -------------------------------------------------------
# Step 6 — Create Gold External Location
# -------------------------------------------------------
print(f"\nStep 6: Creating gold external location...")

spark.sql(f"""
    CREATE EXTERNAL LOCATION IF NOT EXISTS `el-gold-{env}`
    URL 's3://dynamodb-project-exports/db-gold/{env}/'
    WITH (STORAGE CREDENTIAL `s3-dynamodb-exports-credential`)
    COMMENT 'Gold layer external location - {env} environment'
""")
print(f"✅ External location 'el-gold-{env}' ready")

# -------------------------------------------------------
# Step 7 — Verify
# -------------------------------------------------------
print(f"\nStep 7: Verifying setup...")

print(f"\nSchemas in '{catalog}':")
result = spark.sql(f"SHOW SCHEMAS IN {catalog}").collect()
for row in result:
    print(f"   └── {catalog}.{row['databaseName']}")

print(f"\nExternal Locations:")
display(spark.sql("SHOW EXTERNAL LOCATIONS"))

print("\n✅ Setup complete!")