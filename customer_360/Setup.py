# Databricks notebook source
dbutils.widgets.text("catalog_name", "", "Catalog Name")


# COMMAND ----------

catalog_name = dbutils.widgets.get("catalog_name")
if not catalog_name:
    raise Exception("Catalog name is required to run config")
     


# COMMAND ----------

# MAGIC %run "/Workspace/Users/kpasham@lakefusion.ai/CUSTOMER 360 SOLUTION ACCELERATOR/customer360TableCreation"
# MAGIC

# COMMAND ----------

# MAGIC %run "./customer360TableCreation"
# MAGIC

# COMMAND ----------

# MAGIC %run "./Customer360fullload"
# MAGIC