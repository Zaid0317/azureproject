# Databricks notebook source
# MAGIC %md
# MAGIC #   **DimUser**

# COMMAND ----------

df= spark.read.format("parquet")\
    .load("abfss://bronze@storageazurepro.dfs.core.windows.net/DimUser")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # **Autoloader**

# COMMAND ----------

df_user= spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimUser/checkpoint")\
        .option("schemaEvolutionMode","AddNewColumns")\
        .load("abfss://bronze@storageazurepro.dfs.core.windows.net/DimUser")

# COMMAND ----------

print(df_user.isStreaming)

# COMMAND ----------

display(df_user, checkpointLocation="abfss://silver@storageazurepro.dfs.core.windows.net/DimUser/display_checkpoint")


# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_user= df_user.withColumn("user_name", upper(col("user_name")))

# COMMAND ----------

# it is not very easy to imports util in databricks, bcoz u need to add system path.. what is system path??let say my utility is just a variable for eg:

#/Workspace/Users/zaidsiddiqui429@gmail.com/Drafts/spotify_dab/utils/transformation.py

#from spotify_dab.utils.tranformation import var_zaid #it will through an error ModuleNotFoundError: No module named 'spotify_dab'

# so for that we need to make sure out root directory of this project is added in the system path

# COMMAND ----------

import os
import sys

project_path= os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)

from utils.transformation import reusable

# COMMAND ----------

df_user_obj= reusable()

df_user= df_user_obj.dropColumns(df_user,['_rescued_data'])
df_user= df_user.dropDuplicates(['user_id'])

# COMMAND ----------

print(df_user.isStreaming)

# COMMAND ----------

#data engineering is not about handling messy datasets, its about building solutions

# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimUser/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@storageazurepro.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

# MAGIC %md
# MAGIC # **DimArtist**

# COMMAND ----------

df_art= spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimArt/checkpoint")\
        .option("schemaEvolutionMode","AddNewColumns")\
        .load("abfss://bronze@storageazurepro.dfs.core.windows.net/DimArtist")

# COMMAND ----------

display(df_art, checkpointLocation="abfss://silver@storageazurepro.dfs.core.windows.net/DimArt/display_checkpoint")

# COMMAND ----------

print(df_art.isStreaming)

# COMMAND ----------

df_art_obj= reusable()

df_art= df_art_obj.dropColumns(df_art,['_rescued_data'])
df_art= df_art.dropDuplicates(['artist_id'])


# COMMAND ----------

df_art.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimArt/checkpoint")\
    .trigger(once=True)\
    .start("abfss://silver@storageazurepro.dfs.core.windows.net/DimArt/data")

# COMMAND ----------

# MAGIC %md
# MAGIC ## In this particular section we r learning about auto loader, spark structure streaming , how to dump the data incrementally, idempotency nd all

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # lets create a table also in our catalog in databrick, it will give a better performance & optimization techniques...  it will also be more readable

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS spotify_cat.silver.DimArtist
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@storageazurepro.dfs.core.windows.net/DimArtist/data'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS spotify_cat.silver.DimUser
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://silver@storageazurepro.dfs.core.windows.net/DimUser/data'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # DimTrack

# COMMAND ----------

df_track= spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimTrack/checkpoint")\
        .option("schemaEvolutionMode","AddNewColumns")\
        .load("abfss://bronze@storageazurepro.dfs.core.windows.net/DimTrack")

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

df_track= df_track.withColumn("duration_flag",when(col('duration_sec') < 150, "low")\
            .when(col('duration_sec') < 300, "medium")\
            .otherwise("high"))

df_track= df_track.withColumn("track_name", regexp_replace(col('track_name'), '-', ' '))
 
display(df_track, checkpointLocation="abfss://silver@storageazurepro.dfs.core.windows.net/DimTrack/display_checkpoint")

# COMMAND ----------

print(df_track)

# COMMAND ----------

# Ye Spark ka fundamental rule hai: streaming DataFrame ka actual data kabhi directly memory mein laya hi nahi ja sakta bina kisi streaming query (checkpoint wale) ke through.

# COMMAND ----------

df_track.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimTrack/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageazurepro.dfs.core.windows.net/DimTrack/data")\
    .toTable("spotify_cat.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # DimDate

# COMMAND ----------

df_date= spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimDate/checkpoint")\
        .option("schemaEvolutionMode","AddNewColumns")\
        .load("abfss://bronze@storageazurepro.dfs.core.windows.net/DimDate")

# COMMAND ----------

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageazurepro.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cat.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # FactStream
# MAGIC

# COMMAND ----------

df_fact= spark.readStream.format("cloudFiles")\
        .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/FactStream/checkpoint")\
        .option("schemaEvolutionMode","AddNewColumns")\
        .load("abfss://bronze@storageazurepro.dfs.core.windows.net/FactStream")

# COMMAND ----------

df_fact.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@storageazurepro.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path","abfss://silver@storageazurepro.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cat.silver.FactStream")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM spotify_cat.gold.dimtrack
# MAGIC WHERE track_id IN (46,5)

# COMMAND ----------

