# Databricks notebook source
# MAGIC
# MAGIC %run "/Workspace/Users/kpasham@lakefusion.ai/CUSTOMER 360 SOLUTION ACCELERATOR/MultiThreading"
# MAGIC
# MAGIC

# COMMAND ----------

embedding_model_endpoint_name = dbutils.widgets.get("embedding_model_endpoint_name")
num_results = int(dbutils.widgets.get("num_results"))
catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

# COMMAND ----------

dbutils.widgets.text("embedding_model_endpoint_name", "databricks-gte-large-en", "Embedding Model")
dbutils.widgets.text("num_results", "4", "Num Results")
dbutils.widgets.text("catalog_name","")

# COMMAND ----------

notebookPath = "current_notebook_path.replace(current_notebook_path.split("/")[-1],"config/Detect_Duplicates_Main")
    
# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
entities = [ 
    {
        "table_name": f"{catalog_name}.customer_bronze.customer",
        "primary_key": "CUSTOMER_ID",
        "columns_to_exclude": "CUSTOMER_ID,CREATED_DATE,LAST_UPDATED_DATE"
    },
    {
        "table_name": f"{catalog_name}.customer_bronze.customer_addresses",
        "primary_key": "ADDRESS_ID",
        "columns_to_exclude": "ADDRESS_ID,CUSTOMER_ID"
    

    },
    {
        "table_name": f"{catalog_name}.customer_bronze.customer_preferences",
        "primary_key": "PREFERENCE_ID",
        "columns_to_exclude": "PREFERENCE_ID,CUSTOMER_ID"
    },
    {
        "table_name": f"{catalog_name}.customer_bronze.customer_transactions",
        "primary_key": "TRANSACTION_ID",
        "columns_to_exclude": "TRANSACTION_ID,CUSTOMER_ID"
    }
]

# COMMAND ----------

# Get all widget values
embedding_model_endpoint_name = dbutils.widgets.get("embedding_model_endpoint_name")
num_results = dbutils.widgets.get("num_results")
catalog_name = dbutils.widgets.get("catalog_name")

# Validate
if not catalog_name:
    raise Exception("Catalog name is required to run this notebook")

print(f"Catalog: {catalog_name}")
print(f"Num Results: {num_results}")
print(f"Embedding Model: {embedding_model_endpoint_name}")

# COMMAND ----------

notebooks = [
    NotebookData(
        f"{notebookPath}",
        3600,
        {
            "table_name": f'{entity["table_name"]}',
            "columns_to_exclude": f'{entity["columns_to_exclude"]}',
            "primary_key": f'{entity["primary_key"]}',
            "catalog_name": f"{catalog_name}",
            "num_results": f"{num_results}",
            "embedding_model_endpoint_name": f"{embedding_model_endpoint_name}",
        },
    )
    for entity in entities
]

# #Array of instances of NotebookData Class
parallel_thread = 12

try:
    res = parallel_notebooks(notebooks, parallel_thread)
    result = [i.result(timeout=3600) for i in res]  # This is a blocking call.
    print(result)
except NameError as e:
    print(e)
