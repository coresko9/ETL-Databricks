# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

table_name = 'UsCovidData'

spark.conf.set('var.table_name',table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${var.database_name_raw}.${var.table_name}
# MAGIC (
# MAGIC     states STRING,
# MAGIC     positive STRING,
# MAGIC     negative STRING,
# MAGIC     pending STRING,
# MAGIC     hospitalizedCurrently STRING,
# MAGIC     hospitalizedCumulative STRING,
# MAGIC     inIcuCurrently STRING,
# MAGIC     inIcuCumulative STRING,
# MAGIC     onVentilatorCurrently STRING,
# MAGIC     onVentilatorCumulative STRING,
# MAGIC     dateChecked STRING,
# MAGIC     death STRING,
# MAGIC     hospitalized STRING,
# MAGIC     totalTestResults STRING,
# MAGIC     lastModified STRING,
# MAGIC     recovered STRING,
# MAGIC     total STRING,
# MAGIC     posNeg STRING,
# MAGIC     deathIncrease STRING,
# MAGIC     hospitalizedIncrease STRING,
# MAGIC     negativeIncrease STRING,
# MAGIC     positiveIncrease STRING,
# MAGIC     totalTestResultsIncrease STRING,
# MAGIC     hash STRING
# MAGIC )
