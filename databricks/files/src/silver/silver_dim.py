# Databricks notebook source
# MAGIC %md
# MAGIC ## **Autoloader Ingestion**

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimUser silver table ingestion

# COMMAND ----------

# Read the DimUser table via autoloader for idempotency
df_user = spark.readStream.format("cloudFiles")\
                .option("cloudFiles.format", "parquet")\
                .option("cloudFiles.schemaLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimUser/schema")\
                .option("cloudFiles.schemaEvolutionMode", "rescue")\
                .load("abfss://bronze@spotifye2edev.dfs.core.windows.net/dimuser/")

# COMMAND ----------

# Performing some transformations on the data and dropping the unnecessary columns
df_user = df_user.withColumn("user_name", upper(col("user_name"))).drop("_rescued_data")

# COMMAND ----------

# Dropping the duplicate records
df_user = df_user.dropDuplicates(["user_id"])

# COMMAND ----------

# DBTITLE 1,Cell 5
# Writing data to the DimUser delta table in silver schema
df_user.writeStream.format("delta")\
            .outputMode("append")\
            .option("checkpointLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimUser/checkpoints")\
            .trigger(once=True)\
            .option("path", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimUser/data")\
            .toTable("spotify_cata.silver.DimUser")\
            .awaitTermination()

# COMMAND ----------

display(spark.read.table("spotify_cata.silver.dimuser"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist silver table ingestion

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format", "parquet")\
                        .option("cloudFiles.schemaLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimArtist/schema")\
                        .option("cloudFiles.schemaEvolutionMode", "rescue")\
                        .load("abfss://bronze@spotifye2edev.dfs.core.windows.net/dimartist/")

# COMMAND ----------

df_artist = df_artist.dropDuplicates(["artist_id"]).drop("_rescued_data")

# COMMAND ----------

df_artist.writeStream.format("delta")\
                    .outputMode("append")\
                    .option("checkpointLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimArtist/checkpoints")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimArtist/data")\
                    .toTable("spotify_cata.silver.DimArtist")\
                    .awaitTermination()

# COMMAND ----------

display(spark.read.table("spotify_cata.silver.DimArtist"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimTrack silver table ingestion

# COMMAND ----------

display(spark.read.parquet("abfss://bronze@spotifye2edev.dfs.core.windows.net/dimtrack/"))

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format", "parquet")\
                        .option("cloudFiles.schemaLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimTrack/schema")\
                        .option("cloudFiles.schemaEvolutionMode", "rescue")\
                        .load("abfss://bronze@spotifye2edev.dfs.core.windows.net/dimtrack/")

# COMMAND ----------

# DBTITLE 1,Untitled
df_track = df_track.withColumn("duration_category", when(col("duration_sec") < 150, "low")\
                                                    .when(col("duration_sec") < 300, "medium")\
                                                    .otherwise("high"))\
                    .withColumn("track_name", regexp_replace(col("track_name"), "-", " "))\
                    .drop("_rescued_data")

# COMMAND ----------

df_track.writeStream.format("delta")\
                    .outputMode("append")\
                    .option("checkpointLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimTrack/checkpoints")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimTrack/data")\
                    .toTable("spotify_cata.silver.DimTrack")\
                    .awaitTermination()

# COMMAND ----------

display(spark.read.table("spotify_cata.silver.DimTrack"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimDate silver table ingestion

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format", "parquet")\
                        .option("cloudfiles.schemaLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimDate/schema")\
                        .option("cloudFiles.schemaEvolutionMode", "rescue")\
                        .load("abfss://bronze@spotifye2edev.dfs.core.windows.net/dimdate")

# COMMAND ----------

df_date = df_date.drop("_rescued_data")

# COMMAND ----------

df_date.writeStream.format("delta")\
                    .outputMode("append")\
                    .option("checkpointLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimDate/checkpoints")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@spotifye2edev.dfs.core.windows.net/DimDate/data")\
                    .toTable("spotify_cata.silver.DimDate")\
                    .awaitTermination()

# COMMAND ----------

display(spark.read.table("spotify_cata.silver.DimDate"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream silver table ingestion

# COMMAND ----------

df_stream = spark.readStream.format("cloudFiles")\
                        .option("cloudFiles.format", "parquet")\
                        .option("cloudFiles.schemaLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/FactStream/schema")\
                        .option("cloudFiles.schemaEvolutionMode", "rescue")\
                        .load("abfss://bronze@spotifye2edev.dfs.core.windows.net/factstream")

# COMMAND ----------

df_stream = df_stream.drop("_rescued_data")

# COMMAND ----------

df_stream.writeStream.format("delta")\
                    .outputMode("append")\
                    .option("checkpointLocation", "abfss://silver@spotifye2edev.dfs.core.windows.net/FactStream/checkpoints")\
                    .trigger(once=True)\
                    .option("path", "abfss://silver@spotifye2edev.dfs.core.windows.net/FactStream/data")\
                    .toTable("spotify_cata.silver.FactStream")\
                    .awaitTermination()

# COMMAND ----------

display(spark.read.table("spotify_cata.silver.FactStream"))