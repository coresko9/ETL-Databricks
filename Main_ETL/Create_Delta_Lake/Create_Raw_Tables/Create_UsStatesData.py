# Databricks notebook source
# MAGIC %run "Includes/Configuration"

# COMMAND ----------

table_name = 'UsStatesData'

spark.conf.set('var.table_name',table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ${var.database_name_raw}.${var.table_name}
# MAGIC (
# MAGIC     ID_State STRING,
# MAGIC     State STRING,
# MAGIC     ID_Year STRING,
# MAGIC     Year STRING,
# MAGIC     Population STRING,
# MAGIC     Slug_State STRING
# MAGIC )
