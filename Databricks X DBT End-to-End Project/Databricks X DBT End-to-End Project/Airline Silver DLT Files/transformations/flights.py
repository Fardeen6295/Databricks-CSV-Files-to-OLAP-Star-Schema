import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *



@dlt.view(
    name= "transform_flights"
)
def transform_flights():
    df = spark.readStream.format("delta")\
                .load("/Volumes/airline/bronze/volume_bronze/flights/data/")
    df = df.withColumn("flight_date", to_date(col("flight_date")))\
            .withColumn("modified_date", current_timestamp())\
            .drop("_rescued_data")
    return df


rules = {
    "rule_1" : "flight_id IS NOT NULL"
}

dlt.create_streaming_table(
    name= "silver_flights",
    expect_all_or_drop=rules
    )

dlt.create_auto_cdc_flow(
    target="silver_flights",
    source="transform_flights",
    keys=["flight_id"],
    sequence_by="modified_date",
    stored_as_scd_type=1
)





