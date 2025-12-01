# Databricks notebook source
# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook performs the actual deduplication by merging duplicate customer records based on the similarity threshold.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC *   The `2_analyze_duplicate_candidates_customer360` notebook has been successfully executed
# MAGIC *   The `<table_name>_duplicate_candidates` table exists with high-confidence duplicates
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC *   Creates a deduplicated gold table with merged customer records
# MAGIC *   Creates a mapping table showing which records were merged

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

from pyspark.sql.functions import col, first, collect_set, concat_ws, lit, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("table_name", "customer_demo.customer_bronze.customer", "Table Name")
dbutils.widgets.text("primary_key", "customer_id", "Primary Key")
dbutils.widgets.text("similarity_threshold", "0.85", "Similarity Threshold")
dbutils.widgets.dropdown("merge_strategy", "keep_first", ["keep_first", "keep_most_complete"], "Merge Strategy")

catalog_name = dbutils.widgets.get("catalog_name")
table_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
similarity_threshold = float(dbutils.widgets.get("similarity_threshold"))
merge_strategy = dbutils.widgets.get("merge_strategy")

if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

catalog_name_part = table_name.split(".")[0]
table_name_part = table_name.split(".")[-1]
candidate_table_name = f"{table_name}_duplicate_candidates"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Original Data and Analysis Results

# COMMAND ----------

print(f"Loading original data from: {table_name}")
original_data = spark.table(table_name)

print(f"Loading duplicate candidates from: {candidate_table_name}")
duplicate_candidates = spark.table(candidate_table_name)

high_confidence_duplicates = duplicate_candidates.filter(col("search_score") >= similarity_threshold)

print(f"\nOriginal records: {original_data.count():,}")
print(f"High-confidence duplicate pairs (score >= {similarity_threshold}): {high_confidence_duplicates.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Duplicate Groups
# MAGIC
# MAGIC Group all related duplicates together (e.g., if A=B and B=C, then A=B=C form one group)

# COMMAND ----------

# Create edges for both directions (A->B and B->A are the same duplicate relationship)
edges = high_confidence_duplicates.select(
    col("original_id").alias("src"),
    col("potential_duplicate_id").alias("dst")
).union(
    high_confidence_duplicates.select(
        col("potential_duplicate_id").alias("src"),
        col("original_id").alias("dst")
    )
).distinct()

# Build connected components: for each record, collect ALL related IDs
# This ensures if A=B and B=C, then A gets [A,B,C], B gets [A,B,C], C gets [A,B,C]
duplicate_groups = edges.groupBy("src").agg(
    collect_set("dst").alias("duplicate_ids")
).withColumn(
    "duplicate_ids_with_self",
    concat_ws(",", col("src"), concat_ws(",", col("duplicate_ids")))
)

print(f"Created {duplicate_groups.count():,} duplicate groups")
duplicate_groups.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select Master Record for Each Group

# COMMAND ----------


from pyspark.sql.functions import split, explode, min as spark_min

# Explode all IDs in each group and find the minimum ID as master
all_group_members = duplicate_groups.select(
    col("src"),
    explode(split(col("duplicate_ids_with_self"), ",")).alias("member_id")
)

# For each member, find the minimum ID in their group (this becomes the master)
master_mapping = all_group_members.groupBy("member_id").agg(
    spark_min("src").alias("master_id")
).select(
    col("member_id").alias("record_id"),
    col("master_id")
)

print(f"Created master mapping with {master_mapping.count():,} records")
master_mapping.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Mapping Table

# COMMAND ----------


mapping_table_name = f"{table_name}_deduplication_mapping"
print(f"Creating mapping table: {mapping_table_name}")

master_mapping.write.mode("overwrite").saveAsTable(mapping_table_name)

print(f"✓ Mapping table created with {master_mapping.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Deduplicated Gold Table

# COMMAND ----------


# Get list of all master record IDs
master_ids = master_mapping.select("master_id").distinct()

# Only keep records that are masters
deduplicated_data = original_data.join(
    master_ids,
    original_data[primary_key] == master_ids["master_id"],
    "inner"  # Only keep master records
).drop("master_id")

# Add back non-duplicate records (records not in any duplicate group)
records_in_groups = master_mapping.select("record_id").distinct()
non_duplicate_records = original_data.join(
    records_in_groups,
    original_data[primary_key] == records_in_groups["record_id"],
    "left_anti"  # Keep records NOT in duplicate groups
)

# Union masters + non-duplicates = final deduplicated data
deduplicated_data = deduplicated_data.union(non_duplicate_records)

# Create gold table
catalog_name_part = table_name.split(".")[0]
table_name_part = table_name.split(".")[-1]
gold_table_name = f"{catalog_name_part}.gold.{table_name_part}"

print(f"Creating deduplicated gold table: {gold_table_name}")
deduplicated_data.write.mode("overwrite").saveAsTable(gold_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary Statistics

# COMMAND ----------

original_count = original_data.count()
deduplicated_count = deduplicated_data.count()
removed_count = original_count - deduplicated_count
reduction_percentage = (removed_count / original_count * 100) if original_count > 0 else 0

print("\n" + "=" * 80)
print("DEDUPLICATION SUMMARY")
print("=" * 80)
print(f"Original Records:      {original_count:,}")
print(f"Deduplicated Records:  {deduplicated_count:,}")
print(f"Duplicates Removed:    {removed_count:,}")
print(f"Reduction:             {reduction_percentage:.2f}%")
print("=" * 80)
print(f"\n✓ Deduplicated data saved to: {gold_table_name}")
print(f"✓ ID mapping saved to: {mapping_table_name}")
print("=" * 80)


# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook performs the actual deduplication by merging duplicate customer records based on the similarity threshold.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC *   The `2_analyze_duplicate_candidates_customer360` notebook has been successfully executed
# MAGIC *   The `<table_name>_duplicate_candidates` table exists with high-confidence duplicates
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC *   Creates a deduplicated gold table with merged customer records
# MAGIC *   Creates a mapping table showing which records were merged

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import Libraries

# COMMAND ----------

from pyspark.sql.functions import col, first, collect_set, concat_ws, lit, row_number
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC #### Configure Parameters

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("table_name", "customer_demo.customer_bronze.customer", "Table Name")
dbutils.widgets.text("primary_key", "customer_id", "Primary Key")
dbutils.widgets.text("similarity_threshold", "0.85", "Similarity Threshold")
dbutils.widgets.dropdown("merge_strategy", "keep_first", ["keep_first", "keep_most_complete"], "Merge Strategy")

catalog_name = dbutils.widgets.get("catalog_name")
table_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
similarity_threshold = float(dbutils.widgets.get("similarity_threshold"))
merge_strategy = dbutils.widgets.get("merge_strategy")

if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

# Derive catalog and table parts from the table_name
catalog_name_part = table_name.split(".")[0]
table_name_part = table_name.split(".")[-1]
candidate_table_name = f"{table_name}_duplicate_candidates"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load Original Data and Analysis Results

# COMMAND ----------

print(f"Loading original data from: {table_name}")
original_data = spark.table(table_name)

print(f"Loading duplicate candidates from: {candidate_table_name}")
duplicate_candidates = spark.table(candidate_table_name)

high_confidence_duplicates = duplicate_candidates.filter(col("search_score") >= similarity_threshold)

print(f"\nOriginal records: {original_data.count():,}")
print(f"High-confidence duplicate pairs (score >= {similarity_threshold}): {high_confidence_duplicates.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Duplicate Groups
# MAGIC
# MAGIC Group all related duplicates together (e.g., if A=B and B=C, then A=B=C form one group)

# COMMAND ----------

# Create edges for both directions (A->B and B->A are the same duplicate relationship)
edges = high_confidence_duplicates.select(
    col("original_id").alias("src"),
    col("potential_duplicate_id").alias("dst")
).union(
    high_confidence_duplicates.select(
        col("potential_duplicate_id").alias("src"),
        col("original_id").alias("dst")
    )
).distinct()

# Build connected components: for each record, collect ALL related IDs
# This ensures if A=B and B=C, then A gets [A,B,C], B gets [A,B,C], C gets [A,B,C]
duplicate_groups = edges.groupBy("src").agg(
    collect_set("dst").alias("duplicate_ids")
).withColumn(
    "duplicate_ids_with_self",
    concat_ws(",", col("src"), concat_ws(",", col("duplicate_ids")))
)

print(f"Created {duplicate_groups.count():,} duplicate groups")
duplicate_groups.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Select Master Record for Each Group

# COMMAND ----------

from pyspark.sql.functions import split, explode, min as spark_min

# Explode all IDs in each group and find the minimum ID as master
all_group_members = duplicate_groups.select(
    col("src"),
    explode(split(col("duplicate_ids_with_self"), ",")).alias("member_id")
)

# For each member, find the minimum ID in their group (this becomes the master)
master_mapping = all_group_members.groupBy("member_id").agg(
    spark_min("src").alias("master_id")
).select(
    col("member_id").alias("record_id"),
    col("master_id")
)

print(f"Created master mapping with {master_mapping.count():,} records")
master_mapping.show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Mapping Table

# COMMAND ----------

mapping_table_name = f"{table_name}_deduplication_mapping"
print(f"Creating mapping table: {mapping_table_name}")

master_mapping.write.mode("overwrite").saveAsTable(mapping_table_name)

print(f"✓ Mapping table created with {master_mapping.count():,} records")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create Deduplicated Gold Table

# COMMAND ----------

# Get list of all master record IDs
master_ids = master_mapping.select("master_id").distinct()

# Only keep records that are masters
deduplicated_data = original_data.join(
    master_ids,
    original_data[primary_key] == master_ids["master_id"],
    "inner"  # Only keep master records
).drop("master_id")

# Add back non-duplicate records (records not in any duplicate group)
records_in_groups = master_mapping.select("record_id").distinct()
non_duplicate_records = original_data.join(
    records_in_groups,
    original_data[primary_key] == records_in_groups["record_id"],
    "left_anti"  # Keep records NOT in duplicate groups
)

# Union masters + non-duplicates = final deduplicated data
deduplicated_data = deduplicated_data.union(non_duplicate_records)

# Create gold table in <catalog>.customer_gold.<table_name_part>
catalog_name_part = table_name.split(".")[0]
table_name_part = table_name.split(".")[-1]

# Ensure the customer_gold schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name_part}.customer_gold")

gold_table_name = f"{catalog_name_part}.customer_gold.{table_name_part}"

print(f"Creating deduplicated gold table: {gold_table_name}")
deduplicated_data.write.mode("overwrite").saveAsTable(gold_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Summary Statistics

# COMMAND ----------

original_count = original_data.count()
deduplicated_count = deduplicated_data.count()
removed_count = original_count - deduplicated_count
reduction_percentage = (removed_count / original_count * 100) if original_count > 0 else 0

print("\n" + "=" * 80)
print("DEDUPLICATION SUMMARY")
print("=" * 80)
print(f"Original Records:      {original_count:,}")
print(f"Deduplicated Records:  {deduplicated_count:,}")
print(f"Duplicates Removed:    {removed_count:,}")
print(f"Reduction:             {reduction_percentage:.2f}%")
print("=" * 80)
print(f"\n✓ Deduplicated data saved to: {gold_table_name}")
print(f"✓ ID mapping saved to: {mapping_table_name}")
print("=" * 80)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from customer_demo.gold.customer