import dlt
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dlt.table(
    name = "stg_bookings"
)
def stg_bookings():
    df = spark.readStream.format("delta")\
        .load("/Volumes/airline/bronze/volume_bronze/bookings/data/")
    return df


@dlt.view(
    name = "transform_bookings"
)
def transform_bookings():
    df = spark.readStream.table("stg_bookings")
    df = df.withColumn("amount", col("amount").cast(DoubleType()))\
            .withColumn("booking_date", to_date(col("booking_date")))\
            .withColumn("modified_date", current_timestamp())\
            .drop("_rescued_data")
    return df

rules = {
    "rule_1" : "booking_id IS NOT NULL",
    "rule_2" : "passenger_id IS NOT NULL"
}

@dlt.table(
    name = "silver_bookings"
)
@dlt.expect_all_or_drop(rules)
def silver_bookings():
    df = spark.readStream.table("transform_bookings")
    return df





