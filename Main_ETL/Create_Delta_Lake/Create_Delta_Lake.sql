-- Databricks notebook source
-- MAGIC %run "Includes/Configuration"

-- COMMAND ----------

Create Database if not exists ${var.database_name_raw};
Create Database if not exists ${var.database_name_stage};

