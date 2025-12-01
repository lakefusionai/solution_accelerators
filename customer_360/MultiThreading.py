# Databricks notebook source
from concurrent.futures import ThreadPoolExecutor

class NotebookData:

  def __init__(self, path, timeout, parameters = None, retry = 0):

    self.path = path
    self.timeout = timeout
    self.parameters = parameters
    self.retry = retry

  def submit_notebook(notebook):
    # print("Running URL for Table : %s " % (notebook.parameters['url']))
    try:
      if (notebook.parameters):
        return dbutils.notebook.run(notebook.path, notebook.timeout, notebook.parameters)
      else:
        return dbutils.notebook.run(notebook.path, notebook.timeout)
    except Exception as e:
       if notebook.retry < 1:
        print("Failed For : ",notebook.parameters)
        raise
      
    # print("Retrying for : %s " % (notebook.parameters['url']))
    notebook.retry = notebook.retry - 1
    submit_notebook(notebook)

def parallel_notebooks(notebooks, parallel_thread):
    """
        If you create too many notebooks in parallel the driver may crash when you submit all of the jobs at once.
        This code limits the number of parallel notebooks.
    """
    with ThreadPoolExecutor(max_workers = parallel_thread) as ec:
        return [ec.submit(NotebookData.submit_notebook, notebook) for notebook in notebooks]

     

# COMMAND ----------

current_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
