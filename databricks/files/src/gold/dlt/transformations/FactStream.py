from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table
def factstream_stg():
    df = spark.readStream.table("spotify_cata.silver.factstream")
    return df

dp.create_streaming_table(name="factstream")

dp.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = col("stream_timestamp"),
  stored_as_scd_type = 1
)
