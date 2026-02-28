from pyspark import pipelines as dp
from pyspark.sql.functions import *

expectations = {"rule_1" : "user_id is NOT NULL"}

@dp.table
@dp.expect_all_or_drop(expectations)
def dimuser_stg():
    df = spark.readStream.table("spotify_cata.silver.dimuser")
    return df

dp.create_streaming_table(name="dimuser")

dp.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = col("updated_at"),
  stored_as_scd_type = 2
)
