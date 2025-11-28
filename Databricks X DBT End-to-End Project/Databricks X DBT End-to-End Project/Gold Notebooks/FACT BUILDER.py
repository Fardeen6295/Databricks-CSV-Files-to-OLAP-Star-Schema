# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### **PARAMETERS**

# COMMAND ----------

source_schema = "silver"

source_object = "silver_bookings"

cdc_col = "modified_date"

backdated_refresh = ""

fact_table = f"airline.{source_schema}.{source_object}"

target_schema = "gold"

target_object = "FactBookings"

# For UPSERT WE NEED PRIMARY KEY and These column together will be primary key for TRG table
fact_key_cols = ["DimPassengersKey", "DimFlightsKey", "DimAirportsKey", "booking_date"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### DIMENSION TABLES Array of Dictionaries

# COMMAND ----------

dimensions = [
    {
        "table" : f"airline.{target_schema}.DimPassengers",
        "alias" : "DimPassengers",
        "join_keys" : [("passenger_id", "passenger_id")] # (fact_col, dim_col)
    },
    {
        "table" : f"airline.{target_schema}.DimFlights",
        "alias" : "DimFlights",
        "join_keys" : [("flight_id", "flight_id")] # (fact_col, dim_col)
    },
    {
        "table" : f"airline.{target_schema}.DimAirports",
        "alias" : "DimAirports",
        "join_keys" : [("airport_id", "airport_id")] #(fact_col, dim_col)
    }
]

# COMMAND ----------

# Columns you Want to Keep from FACT TABLE (Beside the Surrogate Keys)
fact_columns = ["booking_date", "amount", "modified_date"]

# COMMAND ----------

# last_load_date
if len(backdated_refresh) == 0:
    if spark.catalog.tableExists(f"airline.{target_schema}.{target_object}"):
        last_load_date = spark.sql(f"""
                                   SELECT MAX({cdc_col}) FROM airline.{target_schema}.{target_object}
                                   """).collect()[0][0]
    else:
        last_load_date = "1900-01-01 00:00:00"

else:
    last_load_date = backdated_refresh

last_load_date
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dynamic Fact Builder Function

# COMMAND ----------

fact_alias = 'f'
select_cols = [f"{fact_alias}.{col}" for col in fact_columns]
for dim in dimensions:
    table_full = dim['table']
    alias = dim['alias']
    table_name = table_full.split('.')[-1]
    surrogate_key = f"{alias}.{table_name}Key"
    select_cols.append(surrogate_key)
select_cols


# COMMAND ----------

def generate_fact_query_incremantal(fact_table, dimensions, fact_columns, cdc_col, last_load_date):
  fact_alias = 'f'

  #Base Columns to Select
  select_cols = [f"{fact_alias}.{col}" for col in fact_columns]

  #Build Join Dynamically
  join_clauses = []
  for dim in dimensions:
    table_full = dim['table']
    alias = dim['alias']
    table_name = table_full.split('.')[-1]
    surrogate_key = f"{alias}.{table_name}Key"
    select_cols.append(surrogate_key) # This will Append Surrogate Key with their table names in Select_col list

    # Build ON clause
    on_condition = [
      f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim['join_keys']
    ]

    join_clause = f"LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_condition)
    join_clauses.append(join_clause)


  # FInal Select and Join Cluase
  select_cluase = ",\n ".join(select_cols) 
  joins = "\n".join(join_clauses)

  # WHERE Clause for Incremental FIltering
  where_clause = f"{fact_alias}.{cdc_col} > DATE('{last_load_date}')"

  # Final Query
  query = f"""
      SELECT 
          {select_cluase}
      FROM {fact_table} {fact_alias}
      {joins}
      WHERE {where_clause}
  """.strip()

  return query

# COMMAND ----------

query = generate_fact_query_incremantal(fact_table, dimensions, fact_columns, cdc_col, last_load_date)
print(query)

# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

fact_key_cols_str =  ' AND '.join([f"src.{col} = trg.{col}" for col in fact_key_cols])

# COMMAND ----------

if spark.catalog.tableExists(f"airline.{target_schema}.{target_object}"):
    dlt_obj = DeltaTable.forName(spark, f"airline.{target_schema}.{target_object}")
    dlt_obj.alias("trg").merge(df_fact.alias("src"), fact_key_cols_str)\
                    .whenMatchedUpdateAll(condition=f"src.{cdc_col} >= trg.{cdc_col}")\
                    .whenNotMatchedInsertAll()\
                    .execute()

else:
    df_fact.write.format("delta")\
        .mode("append")\
        .saveAsTable(f"airline.{target_schema}.{target_object}")

# COMMAND ----------

df_fact.groupBy('DimPassengersKey', 'DimFlightsKey', 'DimAirportsKey', 'booking_date').count().filter('count > 1').display()

# COMMAND ----------

