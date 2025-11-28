# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_bookings = spark.read.format("delta")\
        .load("/Volumes/airline/bronze/volume_bronze/bookings/data/")

# COMMAND ----------

df_bookings = df_bookings.withColumn("amount", col("amount").cast(DoubleType()))\
                            .withColumn("booking_date", to_date(col("booking_date")))\
                            .withColumn("modified_date", current_timestamp())\
                            .drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Flights

# COMMAND ----------

df_flights = spark.read.format("delta")\
        .load("/Volumes/airline/bronze/volume_bronze/flights/data/")

# COMMAND ----------

df_flights = df_flights.withColumn("flight_date", to_date(col("flight_date")))\
                        .withColumn("modified_date", current_timestamp())\
                        .drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Passengers

# COMMAND ----------

df_passengers = spark.read.format("delta")\
  .load("/Volumes/airline/bronze/volume_bronze/passengers/data/")

# COMMAND ----------

df_passengers = df_passengers.withColumn("modified_date", current_timestamp())\
                                .withColumn("f_name", when(instr(col("name"), " ") > 0, 
                                    substring(col("name"), 1, instr(col("name"), " ") - 1))
                                        .otherwise(col("name")))\
                                .withColumn("l_name", when(instr(col("name"), " ") > 0, 
                                    substring(col("name"), instr(col("name"), " ") + 1, length(col("name"))))
                                        .otherwise(col("name")))\
                                .drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Airports

# COMMAND ----------

df_airports = spark.read.format("delta")\
    .load("/Volumes/airline/bronze/volume_bronze/airports/data/")

# COMMAND ----------

df_airports = df_airports.withColumn("modified_date", current_timestamp())\
                .drop("_rescued_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Business View

# COMMAND ----------

df_all = (df_bookings
            .join(df_airports, ["airport_id"])\
            .join(df_passengers, ["passenger_id"])\
            .join(df_flights, ["flight_id"])
            .select(
                col("booking_id"),
                col("booking_date"),
                col("f_name"),
                col("nationality"),
                col("airline"),
                col("origin"),
                col("airport_name"),
                col("destination"),
                col("flight_date"),
                col("amount")
                )
)

# COMMAND ----------

display(df_all)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`/Volumes/airline/bronze/volume_bronze/passengers/data/`

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM airline.gold.dimpassengers order by DimPassengersKey DESC

# COMMAND ----------

