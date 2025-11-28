import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.view(
    name="transform_airports"
)
def transform_airports():
    df = spark.readStream.format("delta")\
            .load("/Volumes/airline/bronze/volume_bronze/airports/data/")
    df = df.withColumn("modified_date", current_timestamp())\
            .drop("_rescued_data")
    return df

rules = {
    "rule_1" : "airport_id IS NOT NULL"
}


dlt.create_streaming_table(
    name="silver_airports",
    expect_all_or_drop=rules
)

dlt.create_auto_cdc_flow(
    target="silver_airports",
    source="transform_airports",
    keys=["airport_id"],
    sequence_by="modified_date",
    stored_as_scd_type=1
)











