# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

# MAGIC %run "Includes/Functions"

# COMMAND ----------

table_name = "uscoviddata"
spark.conf.set("var.table_name",table_name)

source_mount_point = blob_mount_us_covid_data_path
source_name = "UsCovidData"

# COMMAND ----------

input_schema = StructType([StructField("api_results",
                                       ArrayType(
                                           StructType([
                                            StructField("date",StringType(),True),
                                            StructField("states",StringType(),True),
                                            StructField("positive",StringType(),True),
                                            StructField("negative",StringType(),True),
                                            StructField("pending",StringType(),True),
                                            StructField("hospitalizedCurrently",StringType(),True),
                                            StructField("hospitalizedCumulative",StringType(),True),
                                            StructField("inIcuCurrently",StringType(),True),
                                            StructField("inIcuCumulative",StringType(),True),
                                            StructField("onVentilatorCurrently",StringType(),True),
                                            StructField("onVentilatorCumulative",StringType(),True),
                                            StructField("dateChecked",StringType(),True),
                                            StructField("death",StringType(),True),
                                            StructField("hospitalized",StringType(),True),
                                            StructField("totalTestResults",StringType(),True),
                                            StructField("lastModified",StringType(),True),
                                            StructField("recovered",StringType(),True),
                                            StructField("total",StringType(),True),
                                            StructField("posNeg",StringType(),True),
                                            StructField("deathIncrease",StringType(),True),
                                            StructField("hospitalizedIncrease",StringType(),True),
                                            StructField("negativeIncrease",StringType(),True),
                                            StructField("positiveIncrease",StringType(),True),
                                            StructField("totalTestResultsIncrease",StringType(),True),
                                            StructField("hash",StringType(),True)]),
                                           True),
                                       True)])


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

df.printSchema()

# COMMAND ----------

#df = df.withColumn("api_results",explode(df["api_results"]))
df = df.select(
df["api_results.states"],
df["api_results.positive"],
df["api_results.negative"],
df["api_results.pending"],
df["api_results.hospitalizedCurrently"],
df["api_results.hospitalizedCumulative"],
df["api_results.inIcuCurrently"],
df["api_results.inIcuCumulative"],
df["api_results.onVentilatorCurrently"],
df["api_results.onVentilatorCumulative"],
df["api_results.dateChecked"],
df["api_results.death"],
df["api_results.hospitalized"],
df["api_results.totalTestResults"],
df["api_results.lastModified"],
df["api_results.recovered"],
df["api_results.total"],
df["api_results.posNeg"],
df["api_results.deathIncrease"],
df["api_results.hospitalizedIncrease"],
df["api_results.negativeIncrease"],
df["api_results.positiveIncrease"],
df["api_results.totalTestResultsIncrease"],
df["api_results.hash"]
)

# COMMAND ----------

df.createOrReplaceTempView("CovidData")

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into ${var.database_name_raw}.${var.table_name}
# MAGIC select * from CovidData

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from rawdata.uscoviddata
