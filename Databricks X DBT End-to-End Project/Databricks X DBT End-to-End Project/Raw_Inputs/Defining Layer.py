# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS airline

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS airline.raw

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME airline.raw.src_data_volume

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/airline/raw/src_data_volume/rawdata/passengers")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS airline.bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS airline.bronze.bronze_volume