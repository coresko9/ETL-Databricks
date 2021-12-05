# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

table_name = 'StatesCovidData'

spark.conf.set('var.table_name',table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${var.database_name_raw}.${var.table_name}
# MAGIC (
# MAGIC     date STRING,
# MAGIC     state STRING,
# MAGIC     positive STRING,
# MAGIC     probableCases STRING,
# MAGIC     negative STRING,
# MAGIC     pending STRING,
# MAGIC     totalTestResultsSource STRING,
# MAGIC     totalTestResults STRING,
# MAGIC     hospitalizedCurrently STRING,
# MAGIC     hospitalizedCumulative STRING,
# MAGIC     inIcuCurrently STRING,
# MAGIC     inIcuCumulative STRING,
# MAGIC     onVentilatorCurrently STRING,
# MAGIC     onVentilatorCumulative STRING,
# MAGIC     recovered STRING,
# MAGIC     lastUpdateEt STRING,
# MAGIC     dateModified STRING,
# MAGIC     checkTimeEt STRING,
# MAGIC     death STRING,
# MAGIC     hospitalized STRING,
# MAGIC     hospitalizedDischarged STRING,
# MAGIC     dateChecked STRING,
# MAGIC     totalTestsViral STRING,
# MAGIC     positiveTestsViral STRING,
# MAGIC     negativeTestsViral STRING,
# MAGIC     positiveCasesViral STRING,
# MAGIC     deathConfirmed STRING,
# MAGIC     deathProbable STRING,
# MAGIC     totalTestEncountersViral STRING,
# MAGIC     totalTestsPeopleViral STRING,
# MAGIC     totalTestsAntibody STRING,
# MAGIC     positiveTestsAntibody STRING,
# MAGIC     negativeTestsAntibody STRING,
# MAGIC     totalTestsPeopleAntibody STRING,
# MAGIC     positiveTestsPeopleAntibody STRING,
# MAGIC     negativeTestsPeopleAntibody STRING,
# MAGIC     totalTestsPeopleAntigen STRING,
# MAGIC     positiveTestsPeopleAntigen STRING,
# MAGIC     totalTestsAntigen STRING,
# MAGIC     positiveTestsAntigen STRING,
# MAGIC     fips STRING,
# MAGIC     positiveIncrease STRING,
# MAGIC     negativeIncrease STRING,
# MAGIC     total STRING,
# MAGIC     totalTestResultsIncrease STRING,
# MAGIC     posNeg STRING,
# MAGIC     dataQualityGrade STRING,
# MAGIC     deathIncrease STRING,
# MAGIC     hospitalizedIncrease STRING,
# MAGIC     hash STRING,
# MAGIC     commercialScore STRING,
# MAGIC     negativeRegularScore STRING,
# MAGIC     negativeScore STRING,
# MAGIC     positiveScore STRING,
# MAGIC     score STRING,
# MAGIC     grade STRING
# MAGIC )
