# Databricks notebook source
dbutils.widgets.text("src","")

# COMMAND ----------

src_value = dbutils.widgets.get("src")

# COMMAND ----------

df_dynamic = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", f"/Volumes/airline/bronze/volume_bronze/{src_value}/checkpoint")\
    .option("cloudFiles.schemaEvolutionMode", "rescue")\
    .load(f"/Volumes/airline/raw/volume_raw/data/{src_value}/")

# COMMAND ----------

df_dynamic.writeStream.format("delta")\
    .outputMode("append")\
    .trigger(once=True)\
    .option("checkpointLocation", f"/Volumes/airline/bronze/volume_bronze/{src_value}/checkpoint")\
    .option("path", f"/Volumes/airline/bronze/volume_bronze/{src_value}/data")\
    .start()

# COMMAND ----------

