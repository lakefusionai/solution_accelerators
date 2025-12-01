# Databricks notebook source
# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_catalog()

# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook identifies potential duplicate customer records within your data using vector similarity search.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC *   A Databricks workspace with access to Databricks Vector Search.
# MAGIC *   A Model Serving endpoint serving an embedding model (e.g., `databricks-gte-large-en`).
# MAGIC *   Bronze tables with the customer data you want to analyze, with appropriately defined `primaryKey`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC *   Creates tables named `<table_name>_duplicate_candidates` for each entity in the provided data model. These tables contain potential duplicate records and their similarity scores.
# MAGIC
# MAGIC This is your first step in identifying and deduplicating your customer data!

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing necessary libraries

# COMMAND ----------

import time
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import concat_ws
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting Model and Search Parameters
# MAGIC These widgets will be used for searching. You will need to tune the *num_results* parameter. If the value is too low there may be records with a similarity of 1.0. The most common approach for fixing this is simply adding more results.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("table_name", "customer_demo.customer_bronze.customer", "Table Name")  
dbutils.widgets.text("primary_key", "customer_id", "Primary Key")
dbutils.widgets.text("columns_to_exclude", "customer_id,created_date,modified_date", "Columns to exclude")
dbutils.widgets.text("embedding_model_endpoint_name", "databricks-gte-large-en", "Embedding Model Endpoint")
dbutils.widgets.text("num_results", "50", "Number of Results")

catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

# COMMAND ----------

embedding_model_endpoint_name = dbutils.widgets.get("embedding_model_endpoint_name")
num_results = int(dbutils.widgets.get("num_results"))
table_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
columns_to_exclude = dbutils.widgets.get("columns_to_exclude").split(",") if dbutils.widgets.get("columns_to_exclude") else []

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating function *create_embedding_source*
# MAGIC This function combines all relevant columns into a single combined text column that is then transformed into a 1024-dimensional vector for similarity comparison.

# COMMAND ----------

def create_embedding_source(source_df, columns_to_exclude):
    """
    Creates the combined text column used for embedding.

    Args:
        source_df: The source DataFrame.
        columns_to_exclude: A list of columns to exclude from the combined text.

    Returns:
        A DataFrame with the 'combined_data' column added, or None if source_df is None.
    """
    if source_df is None:
        print("Warning: create_embedding_source received a None DataFrame. Returning None.")
        return None, None

    columns_to_include = [col for col in source_df.columns if col not in columns_to_exclude]
    embeddingColumn = "combined_data"
    sourceDF = source_df.withColumn(embeddingColumn, concat_ws(",", *columns_to_include))
    return sourceDF, embeddingColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *create_or_get_index*
# MAGIC This function builds the vector index if it doesn't exist, or retrieves it if it does. The vector index converts each customer record into a 1024-dimensional vector for efficient similarity search.
# MAGIC
# MAGIC **NOTE:** This function only identifies candidates. These candidates are not necessarily duplicates, but can be analyzed in the next notebook. You will need to tune *num_results* so a sufficient number of records are returned.

# COMMAND ----------

def create_or_get_index(table_name, embedding_model_endpoint_name, embeddingColumn, primary_key, vectorSearchEndpointName="lakefusion_vs_endpoint"):
    """
    Creates or retrieves the Vector Search index.

    Args:
        table_name: The name of the table to index.
        embedding_model_endpoint_name: The name of the embedding model endpoint.
        embeddingColumn: The name of the column to use for embedding.
        primary_key: The name of the primary key column.
        vectorSearchEndpointName: The name of the vector search endpoint (default: customer_360_vs_endpoint)

    Returns:
        The Vector Search index object and its name.
    """
    vsc = VectorSearchClient(disable_notice=True)
    indexName = f"{table_name}_index"
    index = None

    try:
        print(f"Creating vector index: {indexName}")
        index = vsc.create_delta_sync_index(
            index_name=indexName,
            endpoint_name=vectorSearchEndpointName,
            source_table_name=table_name,
            embedding_source_column=embeddingColumn,
            primary_key=primary_key,
            embedding_model_endpoint_name=embedding_model_endpoint_name,
            pipeline_type="TRIGGERED"
        )
        
        timeout = time.time() + 600  # 10 minutes timeout

        while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):
            print("Waiting for index to be ONLINE...")
            time.sleep(30)
            if time.time() > timeout:
                print("Timeout: Index did not become ONLINE within 10 minutes.")
                raise TimeoutError("Index creation timed out.")
        print("Index is ONLINE")

    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"Index {indexName} already exists. Syncing...")
            index = vsc.get_index(endpoint_name=vectorSearchEndpointName, index_name=indexName)
            index.sync()
            
            while not index.describe().get('status').get('detailed_state').startswith('ONLINE_NO_PENDING_UPDATE'):
                print("Waiting for sync to complete...")
                time.sleep(60)
            print("Index sync complete")
        else:
            print(f"An unexpected error occurred during index creation: {e}")
            raise
            
    return index, indexName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *execute_similarity_search*
# MAGIC This is where the duplicate detection happens. Each customer record is compared to all other records in the vector database. The top *num_results* most similar records are returned with similarity scores.

# COMMAND ----------

def execute_similarity_search(sourceDF, embeddingColumn, primary_key, indexName, num_results):
    """
    Executes the similarity search and returns potential duplicate records.

    Args:
        sourceDF: The source DataFrame.
        embeddingColumn: The name of the column to use for embedding.
        primary_key: The name of the primary key column.
        indexName: The name of the index to search.
        num_results: The number of potential duplicates to return.

    Returns:
        A DataFrame containing potential duplicate records and their similarity scores.
    """

    if sourceDF is None:
        print("Warning: execute_similarity_search received a None DataFrame. Returning None.")
        return None

    sourceDF.createOrReplaceTempView("sourceTempTable")

    matchScoreQuery = f"""WITH updated_data as (
      SELECT
        {primary_key}, {embeddingColumn}
      FROM
        sourceTempTable
    )
      SELECT
        updated_data.{primary_key} as original_id,
        updated_data.{embeddingColumn} as original_combined_data,
        search.{primary_key} as potential_duplicate_id,
        search.{embeddingColumn} as potential_duplicate_combined_data,
        search_score
      FROM
        updated_data,
        LATERAL (
          SELECT
            {primary_key}, {embeddingColumn}, search_score
          FROM
            VECTOR_SEARCH(
              index => "{indexName}",
              query_text => updated_data.{embeddingColumn},
              num_results => {num_results}
            )
          WHERE updated_data.{primary_key} <> {primary_key}
          ORDER BY search_score DESC
        ) AS search
    """

    duplicate_candidate_df = spark.sql(matchScoreQuery)
    return duplicate_candidate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *detect_duplicates*
# MAGIC The main controller function that orchestrates the entire duplicate detection process.

# COMMAND ----------

def detect_duplicates(table_name, primary_key, embedding_model_endpoint_name, columns_to_exclude, num_results, vectorSearchEndpointName="customer_360_vs_endpoint"):
    """
    Detects potential duplicate customer records within a table using vector similarity search.

    Args:
        table_name: The name of the table to deduplicate.
        primary_key: The name of the primary key column.
        embedding_model_endpoint_name: The name of the embedding model endpoint.
        columns_to_exclude: Columns to exclude from combined data.
        num_results: The number of potential duplicates to return.
        vectorSearchEndpointName: The Vector Search Endpoint (default: customer_360_vs_endpoint)

    Returns:
        A DataFrame containing potential duplicates and their similarity scores. Returns None if an error occurs.
    """

    try:
        print(f"Reading table: {table_name}")
        source_df = spark.table(table_name)
        print(f"Table has {source_df.count()} records")
    except AnalysisException as e:
        print(f"Error: Table '{table_name}' not found: {e}")
        return None

    sourceDF, embeddingColumn = create_embedding_source(source_df, columns_to_exclude)
    
    # Create silver table for vector indexing
    catalog_name = table_name.split(".")[0]
    tableName = table_name.split(".")[-1]
    silver_table_name = f"{catalog_name}.customer_silver.{tableName}"
    
    print(f"Writing to silver table: {silver_table_name}")
    sourceDF.write.mode("overwrite").saveAsTable(silver_table_name)
    spark.sql(f"ALTER TABLE {silver_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    if sourceDF is None:
        return None

    index, indexName = create_or_get_index(silver_table_name, embedding_model_endpoint_name, embeddingColumn, primary_key, vectorSearchEndpointName)

    if index is None:
        return None

    print("Executing similarity search...")
    duplicate_candidate_df = execute_similarity_search(sourceDF, embeddingColumn, primary_key, indexName, num_results)

    return duplicate_candidate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main Execution
# MAGIC
# MAGIC Execute the duplicate detection model and create the candidate table.

# COMMAND ----------

print(f"Processing table: {table_name}")
print(f"Primary key: {primary_key}")
print(f"Columns to exclude: {columns_to_exclude}")
print(f"Embedding model: {embedding_model_endpoint_name}")
print(f"Number of results: {num_results}")
print("-" * 80)

duplicate_candidates = detect_duplicates(
    table_name, 
    primary_key, 
    embedding_model_endpoint_name,
    columns_to_exclude, 
    num_results
)

if duplicate_candidates is not None:
    candidate_table_name = f"{table_name}_duplicate_candidates"
    print(f"Writing duplicate candidates to: {candidate_table_name}")
    duplicate_candidates.write.mode("overwrite").saveAsTable(candidate_table_name)
    
    count = duplicate_candidates.count()
    print(f"Found {count} potential duplicate pairs")
    print("✓ Duplicate detection complete!")
else:
    print(f"✗ Error: Skipping {table_name} due to error in detect_duplicates function.")


# COMMAND ----------

# Databricks notebook source
# MAGIC %md
# MAGIC ## Purpose
# MAGIC
# MAGIC This notebook identifies potential duplicate customer records within your data using vector similarity search.
# MAGIC
# MAGIC ## Prerequisites
# MAGIC
# MAGIC *   A Databricks workspace with access to Databricks Vector Search.
# MAGIC *   A Model Serving endpoint serving an embedding model (e.g., `databricks-gte-large-en`).
# MAGIC *   Bronze tables with the customer data you want to analyze, with appropriately defined `primaryKey`
# MAGIC
# MAGIC ## Outputs
# MAGIC
# MAGIC *   Creates tables named `<table_name>_duplicate_candidates` for each entity in the provided data model. These tables contain potential duplicate records and their similarity scores.
# MAGIC
# MAGIC This is your first step in identifying and deduplicating your customer data!

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing necessary libraries

# COMMAND ----------

import time
from databricks.vector_search.client import VectorSearchClient
from pyspark.sql.functions import concat_ws
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

# MAGIC %md
# MAGIC #### Setting Model and Search Parameters
# MAGIC These widgets will be used for searching. You will need to tune the *num_results* parameter. If the value is too low there may be records with a similarity of 1.0. The most common approach for fixing this is simply adding more results.

# COMMAND ----------

dbutils.widgets.text("catalog_name", "", "Catalog Name")
dbutils.widgets.text("table_name", "customer_demo.customer_bronze.customer", "Table Name")  
dbutils.widgets.text("primary_key", "customer_id", "Primary Key")
dbutils.widgets.text("columns_to_exclude", "customer_id,created_date,modified_date", "Columns to exclude")
dbutils.widgets.text("embedding_model_endpoint_name", "databricks-gte-large-en", "Embedding Model Endpoint")
dbutils.widgets.text("num_results", "50", "Number of Results")

catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

# COMMAND ----------

embedding_model_endpoint_name = dbutils.widgets.get("embedding_model_endpoint_name")
num_results = int(dbutils.widgets.get("num_results"))
table_name = dbutils.widgets.get("table_name")
primary_key = dbutils.widgets.get("primary_key")
columns_to_exclude = dbutils.widgets.get("columns_to_exclude").split(",") if dbutils.widgets.get("columns_to_exclude") else []

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating function *create_embedding_source*
# MAGIC This function combines all relevant columns into a single combined text column that is then transformed into a 1024-dimensional vector for similarity comparison.

# COMMAND ----------

def create_embedding_source(source_df, columns_to_exclude):
    """
    Creates the combined text column used for embedding.

    Args:
        source_df: The source DataFrame.
        columns_to_exclude: A list of columns to exclude from the combined text.

    Returns:
        A DataFrame with the 'combined_data' column added, or None if source_df is None.
    """
    if source_df is None:
        print("Warning: create_embedding_source received a None DataFrame. Returning None.")
        return None, None

    columns_to_include = [col for col in source_df.columns if col not in columns_to_exclude]
    embeddingColumn = "combined_data"
    sourceDF = source_df.withColumn(embeddingColumn, concat_ws(",", *columns_to_include))
    return sourceDF, embeddingColumn

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *create_or_get_index*
# MAGIC This function builds the vector index if it doesn't exist, or retrieves it if it does. The vector index converts each customer record into a 1024-dimensional vector for efficient similarity search.
# MAGIC
# MAGIC **NOTE:** This function only identifies candidates. These candidates are not necessarily duplicates, but can be analyzed in the next notebook. You will need to tune *num_results* so a sufficient number of records are returned.

# COMMAND ----------

def create_or_get_index(table_name, embedding_model_endpoint_name, embeddingColumn, primary_key, vectorSearchEndpointName="customer_360_vs_endpoint"):
    """
    Creates or retrieves the Vector Search index.

    Args:
        table_name: The name of the table to index.
        embedding_model_endpoint_name: The name of the embedding model endpoint.
        embeddingColumn: The name of the column to use for embedding.
        primary_key: The name of the primary key column.
        vectorSearchEndpointName: The name of the vector search endpoint (default: customer_360_vs_endpoint)

    Returns:
        The Vector Search index object and its name.
    """
    vsc = VectorSearchClient(disable_notice=True)
    indexName = f"{table_name}_index"
    index = None

    try:
        print(f"Creating vector index: {indexName}")
        index = vsc.create_delta_sync_index(
            index_name=indexName,
            endpoint_name=vectorSearchEndpointName,
            source_table_name=table_name,
            embedding_source_column=embeddingColumn,
            primary_key=primary_key,
            embedding_model_endpoint_name=embedding_model_endpoint_name,
            pipeline_type="TRIGGERED"
        )
        
        timeout = time.time() + 600  # 10 minutes timeout

        while not index.describe().get('status').get('detailed_state').startswith('ONLINE'):
            print("Waiting for index to be ONLINE...")
            time.sleep(30)
            if time.time() > timeout:
                print("Timeout: Index did not become ONLINE within 10 minutes.")
                raise TimeoutError("Index creation timed out.")
        print("Index is ONLINE")
        
        print("Waiting 30 seconds for index to fully register in Unity Catalog...")
        time.sleep(30)
        
        # Verify index is accessible with retries
        for attempt in range(5):
            try:
                verified_index = vsc.get_index(endpoint_name=vectorSearchEndpointName, index_name=indexName)
                print(f"Index verified and accessible")
                index = verified_index
                break
            except Exception as verify_error:
                if attempt < 4:
                    print(f"Index not yet accessible, retrying in 10s... (attempt {attempt+1}/5)")
                    time.sleep(10)
                else:
                    print(f"Warning: Index created but verification failed: {verify_error}")

    except Exception as e:
        if "RESOURCE_ALREADY_EXISTS" in str(e):
            print(f"Index {indexName} already exists. Syncing...")
            index = vsc.get_index(endpoint_name=vectorSearchEndpointName, index_name=indexName)
            index.sync()
            
            while not index.describe().get('status').get('detailed_state').startswith('ONLINE_NO_PENDING_UPDATE'):
                print("Waiting for sync to complete...")
                time.sleep(60)
            print("Index sync complete")
        else:
            print(f"An unexpected error occurred during index creation: {e}")
            raise
            
    return index, indexName

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *execute_similarity_search*
# MAGIC This is where the duplicate detection happens. Each customer record is compared to all other records in the vector database. The top *num_results* most similar records are returned with similarity scores.

# COMMAND ----------

def execute_similarity_search(sourceDF, embeddingColumn, primary_key, indexName, num_results):
    """
    Executes the similarity search and returns potential duplicate records.

    Args:
        sourceDF: The source DataFrame.
        embeddingColumn: The name of the column to use for embedding.
        primary_key: The name of the primary key column.
        indexName: The name of the index to search.
        num_results: The number of potential duplicates to return.

    Returns:
        A DataFrame containing potential duplicate records and their similarity scores.
    """

    if sourceDF is None:
        print("Warning: execute_similarity_search received a None DataFrame. Returning None.")
        return None

    sourceDF.createOrReplaceTempView("sourceTempTable")

    matchScoreQuery = f"""WITH updated_data as (
      SELECT
        {primary_key}, {embeddingColumn}
      FROM
        sourceTempTable
    )
      SELECT
        updated_data.{primary_key} as original_id,
        updated_data.{embeddingColumn} as original_combined_data,
        search.{primary_key} as potential_duplicate_id,
        search.{embeddingColumn} as potential_duplicate_combined_data,
        search_score
      FROM
        updated_data,
        LATERAL (
          SELECT
            {primary_key}, {embeddingColumn}, search_score
          FROM
            VECTOR_SEARCH(
              index => "{indexName}",
              query_text => updated_data.{embeddingColumn},
              num_results => {num_results}
            )
          WHERE updated_data.{primary_key} <> {primary_key}
          ORDER BY search_score DESC
        ) AS search
    """

    duplicate_candidate_df = spark.sql(matchScoreQuery)
    return duplicate_candidate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating *detect_duplicates*
# MAGIC The main controller function that orchestrates the entire duplicate detection process.

# COMMAND ----------

def detect_duplicates(table_name, primary_key, embedding_model_endpoint_name, columns_to_exclude, num_results, vectorSearchEndpointName="customer_360_vs_endpoint"):
    """
    Detects potential duplicate customer records within a table using vector similarity search.

    Args:
        table_name: The name of the table to deduplicate.
        primary_key: The name of the primary key column.
        embedding_model_endpoint_name: The name of the embedding model endpoint.
        columns_to_exclude: Columns to exclude from combined data.
        num_results: The number of potential duplicates to return.
        vectorSearchEndpointName: The Vector Search Endpoint (default: customer_360_vs_endpoint)

    Returns:
        A DataFrame containing potential duplicates and their similarity scores. Returns None if an error occurs.
    """

    try:
        print(f"Reading table: {table_name}")
        source_df = spark.table(table_name)
        print(f"Table has {source_df.count()} records")
    except AnalysisException as e:
        print(f"Error: Table '{table_name}' not found: {e}")
        return None

    sourceDF, embeddingColumn = create_embedding_source(source_df, columns_to_exclude)
    
    # Create silver table for vector indexing
    catalog_name = table_name.split(".")[0]
    tableName = table_name.split(".")[-1]
    silver_table_name = f"{catalog_name}.customer_silver.{tableName}"
    
    print(f"Writing to silver table: {silver_table_name}")
    sourceDF.write.mode("overwrite").saveAsTable(silver_table_name)
    spark.sql(f"ALTER TABLE {silver_table_name} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

    if sourceDF is None:
        return None

    index, indexName = create_or_get_index(silver_table_name, embedding_model_endpoint_name, embeddingColumn, primary_key, vectorSearchEndpointName)

    if index is None:
        return None

    print("Executing similarity search...")
    duplicate_candidate_df = execute_similarity_search(sourceDF, embeddingColumn, primary_key, indexName, num_results)

    return duplicate_candidate_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Main Execution
# MAGIC
# MAGIC Execute the duplicate detection model and create the candidate table.

# COMMAND ----------

print(f"Processing table: {table_name}")
print(f"Primary key: {primary_key}")
print(f"Columns to exclude: {columns_to_exclude}")
print(f"Embedding model: {embedding_model_endpoint_name}")
print(f"Number of results: {num_results}")
print("-" * 80)

duplicate_candidates = detect_duplicates(
    table_name, 
    primary_key, 
    embedding_model_endpoint_name,
    columns_to_exclude, 
    num_results
)

if duplicate_candidates is not None:
    candidate_table_name = f"{table_name}_duplicate_candidates"
    print(f"Writing duplicate candidates to: {candidate_table_name}")
    duplicate_candidates.write.mode("overwrite").saveAsTable(candidate_table_name)
    
    count = duplicate_candidates.count()
    print(f"Found {count} potential duplicate pairs")
    print("✓ Duplicate detection complete!")
else:
    print(f"✗ Error: Skipping {table_name} due to error in detect_duplicates function.")
