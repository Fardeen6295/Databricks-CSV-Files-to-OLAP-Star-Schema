import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *


@dlt.view(
    name = "transform_passengers"
)
def transform_passengers():
    df = spark.readStream.format("delta")\
            .load("/Volumes/airline/bronze/volume_bronze/passengers/data/")
    df = df.withColumn("modified_date", current_timestamp())\
            .withColumn("f_name", when(instr(col("name"), " ") > 0, 
                substring(col("name"), 1, instr(col("name"), " ") - 1))
                    .otherwise(col("name")))\
            .withColumn("l_name", when(instr(col("name"), " ") > 0, 
                substring(col("name"), instr(col("name"), " ") + 1, length(col("name"))))
                    .otherwise(col("name")))\
            .drop("_rescued_data")
    return df

rules = {
    "rule_1" : "passenger_id IS NOT NULL"
}

dlt.create_streaming_table(
    name="silver_passengers",
    expect_all_or_drop=rules
)

dlt.create_auto_cdc_flow(
    target="silver_passengers",
    source="transform_passengers",
    keys=["passenger_id"],
    sequence_by="modified_date",
    stored_as_scd_type=1
)








