# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook filters potential duplicate records based on entity-specific similarity score thresholds, creating a "gold" table.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC * Run notebook 1 to create the Vector Search DB and candidate views.
# MAGIC * Run notebook 2 to compute/validate per-entity thresholds.
# MAGIC * Verify the bronze tables and thresholds below.

# COMMAND ----------

# MAGIC %md
# MAGIC #### dbutils Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

gold_table_root = f"{catalog_name}.gold"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Model Parameters (Customer 360 entities)

# COMMAND ----------

# Entities from your Customer 360 bronze layer
entities = [
    {"table_name": f"{catalog_name}.customer_bronze.customer",                "primary_key": "CUSTOMER_ID", "columns_to_exclude": ["CUSTOMER_ID"]},
    {"table_name": f"{catalog_name}.customer_bronze.customer_addresses",      "primary_key": "CUSTOMER_ID", "columns_to_exclude": ["CUSTOMER_ID"]},
    {"table_name": f"{catalog_name}.customer_bronze.customer_contacts",       "primary_key": "CUSTOMER_ID", "columns_to_exclude": ["CUSTOMER_ID"]},
    {"table_name": f"{catalog_name}.customer_bronze.customer_preferences",    "primary_key": "CUSTOMER_ID", "columns_to_exclude": ["CUSTOMER_ID"]},
    {"table_name": f"{catalog_name}.customer_bronze.customer_transactions",   "primary_key": "CUSTOMER_ID", "columns_to_exclude": ["CUSTOMER_ID"]}
]

# COMMAND ----------

# MAGIC %md
# MAGIC #### Thresholds per entity
# MAGIC Adjust these based on Notebook 2 recommendations.

# COMMAND ----------

thresholds = {
    f"{catalog_name}.customer_bronze.customer":               0.95,
    f"{catalog_name}.customer_bronze.customer_addresses":     0.92,
    f"{catalog_name}.customer_bronze.customer_contacts":      0.91,
    f"{catalog_name}.customer_bronze.customer_preferences":   0.90,
    f"{catalog_name}.customer_bronze.customer_transactions":  0.93
}

# COMMAND ----------

# MAGIC %md
# MAGIC #### Process each entity (Provider-style threshold rule)

# COMMAND ----------

for entity in entities:
    table_name  = entity["table_name"]
    primary_key = entity["primary_key"]
    print(primary_key)

    entity_name     = table_name.split(".")[-1]
    candidate_view  = f"{table_name}_duplicate_candidates"
    gold_table      = f"{gold_table_root}.{entity_name}"

    # Threshold lookup
    if table_name in thresholds:
        threshold = thresholds[table_name]
        print(f"Processing {table_name} with threshold {threshold}")
    else:
        print(f"No threshold found for table {table_name} in the thresholds dictionary. Skipping...")
        continue

    # Ensure source table exists
    try:
        _ = spark.table(table_name)
    except Exception as e:
        print(f"Error: Table '{table_name}' not found. Please check the table name.")
        continue

    # Ensure candidate view exists
    try:
        _ = spark.table(candidate_view)
    except Exception as e:
        print(f"Error: Duplicate candidate view '{candidate_view}' not found. Please check the view name.")
        continue

    # Provider-style filter: keep rows with no dup OR score below threshold
    filtered_data = spark.sql(f"""
        SELECT t1.*
        FROM {table_name} t1
        LEFT JOIN {candidate_view} t2
          ON t1.{primary_key} = t2.original_id
        WHERE t2.original_id IS NULL OR t2.search_score < {threshold}
    """)

    # Write to gold.<entity_name>
    filtered_data.write.mode("overwrite").saveAsTable(gold_table)

    # Drop the temporary candidate view (mirrors provider flow)
    spark.sql(f"DROP TABLE IF EXISTS {candidate_view}")
