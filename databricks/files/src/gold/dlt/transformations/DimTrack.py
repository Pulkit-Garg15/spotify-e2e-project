from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.table
def dimtrack_stg():
    df = spark.readStream.table("spotify_cata.silver.dimtrack")
    return df

dp.create_streaming_table(name="dimtrack")

dp.create_auto_cdc_flow(
  target = "dimtrack",
  source = "dimtrack_stg",
  keys = ["track_id"],
  sequence_by = col("updated_at"),
  stored_as_scd_type = 2
)
