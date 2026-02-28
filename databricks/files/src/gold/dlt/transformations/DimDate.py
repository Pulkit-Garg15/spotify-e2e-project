from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table
def dimdate_stg():
    df = spark.readStream.table("spotify_cata.silver.dimdate")
    return df

dp.create_streaming_table(name="dimdate")

dp.create_auto_cdc_flow(
  target = "dimdate",
  source = "dimdate_stg",
  keys = ["date_key"],
  sequence_by = col("date"),
  stored_as_scd_type = 2
)
