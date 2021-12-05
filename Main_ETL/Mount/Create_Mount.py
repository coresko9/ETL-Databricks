# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

# MAGIC %run "Includes/Functions"

# COMMAND ----------

# DBTITLE 1,Mount Us Covid Data
mount(blob_abfss_us_covid_data_path,blob_mount_us_covid_data_path)

# COMMAND ----------

# DBTITLE 1,Mount States Covid Data
mount(blob_abfss_states_covid_data_path,blob_mount_states_covid_data_path)

# COMMAND ----------

# DBTITLE 1,Mount Us States Data
mount(blob_abfss_us_states_data_path,blob_mount_us_states_data_path)

# COMMAND ----------

# DBTITLE 1,Mount Test Files
mount(blob_abfss_test_files_path,blob_mount_test_files_path)

# COMMAND ----------

#a = "dbfs:/mnt/blob/TestFiles/"
#b = "dbfs:/mnt/blob/UsCovidData/"
#dbutils.fs.unmount(a)
#dbutils.fs.unmount(b)
#
#dbutils.fs.rm(a,True)
#dbutils.fs.rm(b,True)

# COMMAND ----------

# MAGIC %fs ls "/mnt/blob"
