# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### REQUIRED PARAMETERS Fill By User

# COMMAND ----------

source_schema = "silver"

source_object = "silver_bookings"

cdc_col = "modified_date"

backdated_refresh = ""

target_schema = "gold"

target_object = "FactBookings"

fact_table = f"airline.{source_schema}.{source_object}" # source fact table in silver

fact_key_cols = ["DimPassengersKey", "DimAirportsKey", "DimFlightsKey", "booking_date"]
# Together all of them Act as primary key for fact table required to perform upsert
# Combination should garantee unique for each row

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dimension Tables Details and Fact Columns to get Fill by User

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
        "join_keys" : [("airport_id", "airport_id")] # (fact_col, dim_col)
    }
]

# COMMAND ----------

# Fact Columns to get beside Surrogate Keys from Dimensions 
fact_columns = ["booking_date", "amount", "modified_date"] 

# COMMAND ----------

# MAGIC %md
# MAGIC Last Load date for Incremental Load

# COMMAND ----------

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
# MAGIC #### Function that will create query for fact table with incremnetal load

# COMMAND ----------

def generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load_date):
    fact_alias = "f"

    select_cols = [f"{fact_alias}.{col}" for col in fact_columns] # Result as f.amount, f.booking-date

    join_clauses = []
    for dim in dimensions:
        table_full = dim["table"]
        alias = dim["alias"]
        table_name = table_full.split(".")[-1]
        surrogate_key = f"{alias}.{table_name}Key"
        select_cols.append(surrogate_key) 
        # Dimension cols with alias and append to select_col for complete list


        # On Clause which will use join_keys from dimensions
        on_condition = [f"{fact_alias}.{fk} = {alias}.{dk}" for fk, dk in dim["join_keys"]]

        # Join Condition which will join fact and dims
        join_clause = f" LEFT JOIN {table_full} {alias} ON " + " AND ".join(on_condition)
        join_clauses.append(join_clause)

    select_cols = ",\n     ".join(select_cols)
    joins = "\n".join(join_clauses)

    # Where Clause 
    where_cluase = f"{fact_alias}.{cdc_col} > DATE('{last_load_date}')"

    # Final Query
    query = f"""
    SELECT 
        {select_cols}
    FROM {fact_table} {fact_alias}
    {joins}
    WHERE {where_cluase}
    """.strip()

    return query



# COMMAND ----------

query = generate_fact_query_incremental(fact_table, dimensions, fact_columns, cdc_col, last_load_date)
print(query)


# COMMAND ----------

df_fact = spark.sql(query)

# COMMAND ----------

