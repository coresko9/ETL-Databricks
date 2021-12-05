# Databricks notebook source
import requests
import json
from datetime import datetime, timedelta
import urllib
import time
from pyspark.sql.types import *
from pyspark.sql.functions import *


# COMMAND ----------

application_id = dbutils.secrets.get(scope="kvetlsandbox", key = "App-ID")
sp_client_secret = dbutils.secrets.get(scope="kvetlsandbox", key = "Client-Secret")
tenant_id = dbutils.secrets.get(scope="kvetlsandbox", key = "Tenant-ID")

storage_account_name = "coreyetlsandboxblob"

# COMMAND ----------

mount_configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": application_id,
          "fs.azure.account.oauth2.client.secret": sp_client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

database_name_raw = "rawdata"
database_name_stage = "stagedata"

spark.conf.set('var.database_name_raw',database_name_raw)
spark.conf.set('var.database_name_stage',database_name_stage)


test_files_container_name = "test-files"
test_files_mount = "TestFiles"

us_covid_data_container_name = "us-covid-data"
us_covid_data_mount = "UsCovidData"

states_covid_data_container_name = "states-covid-data"
states_covid_data_mount = "StatesCovidData"

us_states_data_container_name = "us-states-data"
us_states_data_mount = "UsStatesData"

# COMMAND ----------

blob_abfss_test_files_path = f"abfss://{test_files_container_name}@{storage_account_name}.dfs.core.windows.net/"
blob_mount_test_files_path = f"/mnt/blob/{test_files_mount}/"

blob_abfss_us_covid_data_path = f"abfss://{us_covid_data_container_name}@{storage_account_name}.dfs.core.windows.net/"
blob_mount_us_covid_data_path = f"/mnt/blob/{us_covid_data_mount}/"

blob_abfss_states_covid_data_path = f"abfss://{states_covid_data_container_name}@{storage_account_name}.dfs.core.windows.net/"
blob_mount_states_covid_data_path = f"/mnt/blob/{states_covid_data_mount}/"

blob_abfss_us_states_data_path = f"abfss://{us_states_data_container_name}@{storage_account_name}.dfs.core.windows.net/"
blob_mount_us_states_data_path = f"/mnt/blob/{us_states_data_mount}/"
