# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

# MAGIC %run "Includes/Functions"

# COMMAND ----------

table_name = "usstatesdata"
spark.conf.set("var.table_name",table_name)

source_mount_point = blob_mount_us_states_data_path
source_name = "UsStatesData"

# COMMAND ----------

input_schema = StructType([
                        StructField("api_results",ArrayType(
                               StructType([
                                   StructField("data",ArrayType(StructType([
                                       StructField("ID State",StringType(),True),
                                       StructField("State",StringType(),True),
                                       StructField("ID Year",StringType(),True),
                                       StructField("Year",StringType(),True),
                                       StructField("Population",StringType(),True),
                                       StructField("Slug State",StringType(),True)
                                   ])),True)]),True),True)])
                                              
                                   

# COMMAND ----------

param_process_time = dbutils.widgets.get("Process_Time")
param_process_time = param_process_time.replace("T"," ")
param_process_time = param_process_time.replace("Z","")
param_process_time = param_process_time[:len(param_process_time)-2]
process_time = datetime.strptime(param_process_time,"%Y-%m-%d %H:%M:%S.%f")
folder_mount_path =  process_time.strftime("%Y/%m/%d/%H/")
file_name_datetime = process_time.strftime("-%Y-%m-%d-%H-%M")
file_name = f"{source_name}{file_name_datetime}.json"

full_mount_path = f"{source_mount_point}{folder_mount_path}{file_name}"

# COMMAND ----------

df = spark.read.schema(input_schema).format('org.apache.spark.sql.json').load(full_mount_path)


# COMMAND ----------

df = df.withColumn("api_results", explode(df["api_results"]))
df = df.withColumn("_data", explode(df["api_results.data"]))
df = df.select(
df["_data.ID State"].alias("ID_State"),
df["_data.State"],
df["_data.ID Year"].alias("ID_Year"),
df["_data.Year"],
df["_data.Population"],
df["_data.Slug State"].alias("Slug_State")
)

# COMMAND ----------

df.createOrReplaceTempView("StatesData")

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into ${var.database_name_raw}.${var.table_name}
# MAGIC select * from StatesData
