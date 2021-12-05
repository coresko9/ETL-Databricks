# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

# MAGIC %run "Includes/Functions"

# COMMAND ----------

process_time = dbutils.widgets.get("Process_Time")
process_time = process_time.replace("T"," ")
process_time = process_time.replace("Z","")
process_time = process_time[:len(process_time)-2]

# COMMAND ----------

process_time = datetime.strptime(process_time,"%Y-%m-%d %H:%M:%S.%f")

# COMMAND ----------

folder_path_date = process_time.strftime("%Y/%m/%d/%H/")
file_name_datetime = process_time.strftime("-%Y-%m-%d-%H-%M")
file_name = f"StatesCovidData{file_name_datetime}.json"

mount_path = f"{blob_mount_states_covid_data_path}{folder_path_date}"

dbutils.fs.mkdirs(mount_path)

dbfs_path = f"/dbfs{mount_path}"

# COMMAND ----------

covid_base_url = "https://api.covidtracking.com/v1/states/daily.json"
response_str = ""

# COMMAND ----------

response = get_api(covid_base_url)
response_str += "{\"api_results\":"
response_str += str(json.dumps(response))
response_str += "}"
with open(dbfs_path + file_name, 'w') as f:
    f.write(response_str)
