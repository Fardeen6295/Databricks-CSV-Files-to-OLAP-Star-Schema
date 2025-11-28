# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable
from pyspark.sql.window import Window


# COMMAND ----------

# MAGIC %md
# MAGIC ## PARAMETERS

# COMMAND ----------

#KEY COLUMNS LIST
key_cols = "['flight_id']"
key_cols_list = eval(key_cols)

#cdc_col
cdc_col = "modified_date"

# BACHDATED REFRESH (BACKFILL)
backdated_refresh = ""

# SOURCE SCHEMA
source_schema = "silver"

# SOURCE OBJECT
source_object = "silver_flights"

# TARGET SCHEMA
target_schema = "gold"

# TARGET OBJECT
target_object = "DimFlights"

# SURROGATE KEY
surrogate_key = "DimFlightsKey"

# COMMAND ----------

# MAGIC %md
# MAGIC ## INCREMENTAL LOAD

# COMMAND ----------

# MAGIC %md
# MAGIC #### Last Load Date for Incremental Load of Data 

# COMMAND ----------

if len(backdated_refresh) == 0:
    if spark.catalog.tableExists(f"airline.{target_schema}.{target_object}"):
        last_load_date = spark.sql(f"""
                                   SELECT MAX({cdc_col}) FROM airline.{target_schema}.{target_object}
                                   """).collect()[0][0]
    else:
        last_load_date = '1900-01-01 00:00:00'

else:
    last_load_date = backdated_refresh

last_load_date

# COMMAND ----------

df_src = spark.sql(f"""
                   SELECT * FROM airline.{source_schema}.{source_object} WHERE {cdc_col} > '{last_load_date}'
                   """)

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## OLD vs NEW RECORDS

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plan
# MAGIC * Get Old Records from Target (Already Exsisting table) in df_trg only required cols
# MAGIC * If its the first run then df_trg will be a sudo dataframe of trg required columns
# MAGIC * Required columns from trg are key_columns, surrogate key, create_date, update_date
# MAGIC * Create df_join by joining df_src (Incrementally Loaded Records) with df_trg (Already Present Data in Gold)
# MAGIC * Create df_old and df_new dataframes based on surrogate key col (Null = NEW , Not Null = OLD)

# COMMAND ----------

key_cols_list_string = ", ".join(key_cols_list)
key_cols_list_string

# COMMAND ----------

key_cols_string_init = [f" '' AS {i} " for i in key_cols_list]
key_cols_string_init = ", ".join(key_cols_string_init)
key_cols_string_init

# COMMAND ----------

if spark.catalog.tableExists(f"airline.{target_schema}.{target_object}"):
    
    key_cols_string_incremental = ", ".join(key_cols_list)
    # Result as 'flight_id, flight_name' for SQL Query

    df_trg = spark.sql(f"""
                       SELECT {key_cols_string_incremental}, {surrogate_key}, create_date, update_date
                       FROM airline.{target_schema}.{target_object}
                       """)
else:
    key_cols_string_initial = [f" '' AS {i}" for i in key_cols_list]
    key_cols_string_initial = ", ".join(key_cols_string_initial)
    # Result as " '' AS flight_id ,  '' AS flight_name " for sudo dataframe in SQL Query
    df_trg = spark.sql(f"""
                       SELECT {key_cols_string_initial}, 
                       CAST(0 AS INT) AS {surrogate_key},
                       CAST('1900-01-01 00:00:00' as TIMESTAMP) as create_date,
                       CAST('1900-01-01 00:00:00' as TIMESTAMP) as update_date
                       WHERE 1=0
                       """)


# COMMAND ----------

# MAGIC %md
# MAGIC **JOIN CONDITION**

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

join_condition = ' AND '.join([f"src.{i} = trg.{i}" for i in key_cols_list])

df_join = spark.sql(f"""
                    SELECT 
                        src.*,
                        trg.{surrogate_key},
                        trg.create_date,
                        trg.update_date
                    FROM src
                    LEFT JOIN
                    trg ON {join_condition}""")

# COMMAND ----------

df_join.display()

# COMMAND ----------

df_old = df_join.filter(col(f'{surrogate_key}').isNotNull())

df_new = df_join.filter(col(f'{surrogate_key}').isNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## ENRICH Old and New DFs
# MAGIC * df_old, just need to change update_date with current_timestamp cause its updated now.
# MAGIC * df_new, here we will create surrogate key (Dim Key)
# MAGIC * In First run Surrogate key will start from 0
# MAGIC * In subsequent runs Surrogate key for new rows will start from what max is in trg table + 1 
# MAGIC * Then Union of df_new_enr and df_old_enr

# COMMAND ----------

# Enriching Already Existing rows (have surrogate keys)

df_old_enr = df_old.withColumn("update_date", current_timestamp())

# COMMAND ----------

# Enriching New rows (with new surrogate keys, create_date, update_date

# When table is their, then max key from table and start surrogate from maxkey + 1
if spark.catalog.tableExists(f"airline.{target_schema}.{target_object}"):
    w = Window.orderBy(monotonically_increasing_id())
    max_surrogate_key = spark.sql(f"""
                                  SELECT max({surrogate_key}) FROM airline.{target_schema}.{target_object}
                                  """).collect()[0][0]
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)
                                   + row_number().over(w))\
                                    .withColumn("create_date", current_timestamp())\
                                    .withColumn("update_date", current_timestamp())

# When it is initial run
else:
    max_surrogate_key = 0
    w = Window.orderBy(monotonically_increasing_id())
    df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key) 
                                   + row_number().over(w))\
                                    .withColumn("create_date", current_timestamp())\
                                    .withColumn("update_date", current_timestamp())


# COMMAND ----------

# MAGIC %md
# MAGIC #### UNION of df_new_enr and df_old_enr

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

# MAGIC %md
# MAGIC ## CREATION & UPSERT
# MAGIC * When subsequent run then UPSERT
# MAGIC * When Initial Run then Write(Create) the trg table in Gold
# MAGIC * With Upsert we create a dlt object to facilitate UPSERT

# COMMAND ----------

if spark.catalog.tableExists(f"airline.{target_schema}.{target_object}"):
  dlt_obj = DeltaTable.forName(spark, f"airline.{target_schema}.{target_object}")

  
  dlt_obj.alias("trg").merge(df_union.alias("src"), f"trg.{surrogate_key} = src.{surrogate_key}")\
                  .whenMatchedUpdateAll(condition=f"src.{cdc_col} >= trg.{cdc_col}")\
                  .whenNotMatchedInsertAll()\
                  .execute()
                
else:
  df_union.write.format("delta")\
    .mode("append")\
    .saveAsTable(f"airline.{target_schema}.{target_object}")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM airline.gold.dimairports

# COMMAND ----------

