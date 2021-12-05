# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

# MAGIC %run "Includes/Functions"

# COMMAND ----------

table_name = "statescoviddata"
spark.conf.set("var.table_name",table_name)

source_mount_point = blob_mount_states_covid_data_path
source_name = "StatesCovidData"

# COMMAND ----------

input_schema = StructType([StructField("api_results",
                                       ArrayType(
                                           StructType([
                                            StructField("date",StringType(),True),
                                            StructField("state",StringType(),True),
                                            StructField("positive",StringType(),True),
                                            StructField("probableCases",StringType(),True),
                                            StructField("negative",StringType(),True),
                                            StructField("pending",StringType(),True),
                                            StructField("totalTestResultsSource",StringType(),True),
                                            StructField("totalTestResults",StringType(),True),
                                            StructField("hospitalizedCurrently",StringType(),True),
                                            StructField("hospitalizedCumulative",StringType(),True),
                                            StructField("inIcuCurrently",StringType(),True),
                                            StructField("inIcuCumulative",StringType(),True),
                                            StructField("onVentilatorCurrently",StringType(),True),
                                            StructField("onVentilatorCumulative",StringType(),True),
                                            StructField("recovered",StringType(),True),
                                            StructField("lastUpdateEt",StringType(),True),
                                            StructField("dateModified",StringType(),True),
                                            StructField("checkTimeEt",StringType(),True),
                                            StructField("death",StringType(),True),
                                            StructField("hospitalized",StringType(),True),
                                            StructField("hospitalizedDischarged",StringType(),True),
                                            StructField("dateChecked",StringType(),True),
                                            StructField("totalTestsViral",StringType(),True),
                                            StructField("positiveTestsViral",StringType(),True),
                                            StructField("negativeTestsViral",StringType(),True),
                                            StructField("positiveCasesViral",StringType(),True),
                                            StructField("deathConfirmed",StringType(),True),
                                            StructField("deathProbable",StringType(),True),
                                            StructField("totalTestEncountersViral",StringType(),True),
                                            StructField("totalTestsPeopleViral",StringType(),True),
                                            StructField("totalTestsAntibody",StringType(),True),
                                            StructField("positiveTestsAntibody",StringType(),True),
                                            StructField("negativeTestsAntibody",StringType(),True),
                                            StructField("totalTestsPeopleAntibody",StringType(),True),
                                            StructField("positiveTestsPeopleAntibody",StringType(),True),
                                            StructField("negativeTestsPeopleAntibody",StringType(),True),
                                            StructField("totalTestsPeopleAntigen",StringType(),True),
                                            StructField("positiveTestsPeopleAntigen",StringType(),True),
                                            StructField("totalTestsAntigen",StringType(),True),
                                            StructField("positiveTestsAntigen",StringType(),True),
                                            StructField("fips",StringType(),True),
                                            StructField("positiveIncrease",StringType(),True),
                                            StructField("negativeIncrease",StringType(),True),
                                            StructField("total",StringType(),True),
                                            StructField("totalTestResultsIncrease",StringType(),True),
                                            StructField("posNeg",StringType(),True),
                                            StructField("dataQualityGrade",StringType(),True),
                                            StructField("deathIncrease",StringType(),True),
                                            StructField("hospitalizedIncrease",StringType(),True),
                                            StructField("hash",StringType(),True),
                                            StructField("commercialScore",StringType(),True),
                                            StructField("negativeRegularScore",StringType(),True),
                                            StructField("negativeScore",StringType(),True),
                                            StructField("positiveScore",StringType(),True),
                                            StructField("score",StringType(),True),
                                            StructField("grade",StringType(),True)
                                           ]),
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
df["api_results.date"],
df["api_results.state"],
df["api_results.positive"],
df["api_results.probableCases"],
df["api_results.negative"],
df["api_results.pending"],
df["api_results.totalTestResultsSource"],
df["api_results.totalTestResults"],
df["api_results.hospitalizedCurrently"],
df["api_results.hospitalizedCumulative"],
df["api_results.inIcuCurrently"],
df["api_results.inIcuCumulative"],
df["api_results.onVentilatorCurrently"],
df["api_results.onVentilatorCumulative"],
df["api_results.recovered"],
df["api_results.lastUpdateEt"],
df["api_results.dateModified"],
df["api_results.checkTimeEt"],
df["api_results.death"],
df["api_results.hospitalized"],
df["api_results.hospitalizedDischarged"],
df["api_results.dateChecked"],
df["api_results.totalTestsViral"],
df["api_results.positiveTestsViral"],
df["api_results.negativeTestsViral"],
df["api_results.positiveCasesViral"],
df["api_results.deathConfirmed"],
df["api_results.deathProbable"],
df["api_results.totalTestEncountersViral"],
df["api_results.totalTestsPeopleViral"],
df["api_results.totalTestsAntibody"],
df["api_results.positiveTestsAntibody"],
df["api_results.negativeTestsAntibody"],
df["api_results.totalTestsPeopleAntibody"],
df["api_results.positiveTestsPeopleAntibody"],
df["api_results.negativeTestsPeopleAntibody"],
df["api_results.totalTestsPeopleAntigen"],
df["api_results.positiveTestsPeopleAntigen"],
df["api_results.totalTestsAntigen"],
df["api_results.positiveTestsAntigen"],
df["api_results.fips"],
df["api_results.positiveIncrease"],
df["api_results.negativeIncrease"],
df["api_results.total"],
df["api_results.totalTestResultsIncrease"],
df["api_results.posNeg"],
df["api_results.dataQualityGrade"],
df["api_results.deathIncrease"],
df["api_results.hospitalizedIncrease"],
df["api_results.hash"],
df["api_results.commercialScore"],
df["api_results.negativeRegularScore"],
df["api_results.negativeScore"],
df["api_results.positiveScore"],
df["api_results.score"],
df["api_results.grade"]
)

# COMMAND ----------

df.createOrReplaceTempView("CovidData")

# COMMAND ----------

# MAGIC %sql
# MAGIC Insert into ${var.database_name_raw}.${var.table_name}
# MAGIC select * from CovidData

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from rawdata.statescoviddata
