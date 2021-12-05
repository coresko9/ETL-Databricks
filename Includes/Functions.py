# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

def mount(abfss_path, mount_path):
    dbutils.fs.mount(
      source = abfss_path,
      mount_point = mount_path,
      extra_configs = mount_configs)

# COMMAND ----------

def get_api(url):
    response = requests.get(url)
    return response.json()
