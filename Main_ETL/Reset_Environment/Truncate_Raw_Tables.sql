-- Databricks notebook source
-- MAGIC %run "Includes/Configuration"

-- COMMAND ----------

truncate table ${var.database_name_raw}.uscoviddata;
refresh table ${var.database_name_raw}.uscoviddata;

truncate table  ${var.database_name_raw}.statescoviddata;
refresh table ${var.database_name_raw}.statescoviddata;

truncate table ${var.database_name_raw}.usstatesdata;
refresh table ${var.database_name_raw}.usstatesdata;
