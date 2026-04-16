# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0
# MAGIC
# MAGIC from pyspark.sql.functions import * 
# MAGIC from pyspark.sql.types import *
# MAGIC import os
# MAGIC import sys
# MAGIC import uuid
# MAGIC
# MAGIC project_path = os.path.join(os.getcwd(), '..','..')
# MAGIC
# MAGIC sys.path.append(project_path)
# MAGIC
# MAGIC from utils.transformations import reusable

# COMMAND ----------

# function created for displaying a Streaming Dataframe. (created by me)
def dis(df):
    checkpoint_path = f"/Volumes/spotify_cata/silver/general_checkpoints/{uuid.uuid4()}"
    dbutils.fs.mkdirs(checkpoint_path)
    display(df, checkpointLocation = checkpoint_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimUser**

# COMMAND ----------

# df = spark.read.format('parquet').load('abfss://bronze@datalakespotifyazureprj.dfs.core.windows.net/DimUser/')
# df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### **AUTOLOADER**

# COMMAND ----------

checkpoint_path = f"/Volumes/spotify_cata/silver/general_checkpoints/{uuid.uuid4()}"
dbutils.fs.mkdirs(checkpoint_path)
df_user = spark.readStream.format('cloudFiles')\
                      .option('cloudFiles.format', 'parquet')\
                      .option('cloudFiles.schemaLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimUser/checkpoint')\
                      .option('schemaEvolutionMode', 'addNewColumns')\
                      .option("checkpointLocation", checkpoint_path)\
                      .load('abfss://bronze@datalakespotifyazureprj.dfs.core.windows.net/DimUser/')

# COMMAND ----------

# display(df_user, checkpointLocation="/Volumes/spotify_cata/silver/general_checkpoint")
# dis(df_user)

# COMMAND ----------

df_user = df_user.withColumn('user_name', upper(col('user_name')))

# dis(df_user)

# COMMAND ----------

df_user_obj = reusable()
# Note the function .dropColumns is from Class 'reusable' which we created in tranformations.py file. In Pyspark we have no such function available with that name.
df_user = df_user_obj.dropColumns(df_user, ['_rescued_data'])
df_user = df_user.dropDuplicates(['user_id'])
# dis(df_user)

# COMMAND ----------

# Use this if you simply want to write your data to ADLS
df_user.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimUser/checkpoint')\
        .trigger(once=True)\
        .start('abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimUser/data')

# COMMAND ----------

# Use this if you want to create a table here in Unity Catalog as well as store the table's data to external location i.e on ADLS Gen 2
df_user.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimUser/checkpoint')\
        .trigger(once=True)\
        .option('path', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimUser/data')\
        .toTable('spotify_cata.silver.DimUser')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimArtist**

# COMMAND ----------

checkpoint_path = f"/Volumes/spotify_cata/silver/general_checkpoints/{uuid.uuid4()}"
dbutils.fs.mkdirs(checkpoint_path)
df_artist = spark.readStream.format('cloudFiles')\
                      .option('cloudFiles.format', 'parquet')\
                      .option('cloudFiles.schemaLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimArtist/checkpoint')\
                      .option('schemaEvolutionMode', 'addNewColumns')\
                      .option("checkpointLocation", checkpoint_path)\
                      .load('abfss://bronze@datalakespotifyazureprj.dfs.core.windows.net/DimArtist/')

# COMMAND ----------

    # dis(df_artist)

# COMMAND ----------

df_artist_obj = reusable()
df_artist = df_artist_obj.dropColumns(df_artist, ['_rescued_data'])
df_artist = df_artist.dropDuplicates(['artist_id'])
# dis(df_artist)

# COMMAND ----------

# Use this if you simply want to write your data to ADLS
df_artist.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimArtist/checkpoint')\
        .trigger(once=True)\
        .start('abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimArtist/data')

# COMMAND ----------

# Use this if you want to create a table here in Unity Catalog as well as store the table's data to external location i.e on ADLS Gen 2
df_artist.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimArtist/checkpoint')\
        .trigger(once=True)\
        .option('path', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimArtist/data')\
        .toTable('spotify_cata.silver.DimArtist')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimTrack**

# COMMAND ----------

checkpoint_path = f"/Volumes/spotify_cata/silver/general_checkpoints/{uuid.uuid4()}"
dbutils.fs.mkdirs(checkpoint_path)
df_track = spark.readStream.format('cloudFiles')\
                      .option('cloudFiles.format', 'parquet')\
                      .option('cloudFiles.schemaLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimTrack/checkpoint')\
                      .option('schemaEvolutionMode', 'addNewColumns')\
                      .option("checkpointLocation", checkpoint_path)\
                      .load('abfss://bronze@datalakespotifyazureprj.dfs.core.windows.net/DimTrack/')

# COMMAND ----------

# dis(df_track)

# COMMAND ----------

df_track = df_track.withColumn('duration_flag', when(col('duration_sec')<150, "low")\
                                                .when(col('duration_sec')<300, "medium")\
                                                .otherwise("high"))
df_track = df_track.withColumn('track_name', regexp_replace(col('track_name'), '-', " "))
df_track = reusable().dropColumns(df_track, ['_rescued_data'])
dis(df_track)

# COMMAND ----------

# Use this if you want to create a table here in Unity Catalog as well as store the table's data to external location i.e on ADLS Gen 2
df_track.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimTrack/checkpoint')\
        .trigger(once=True)\
        .option('path', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimTrack/data')\
        .toTable('spotify_cata.silver.DimTrack')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **DimDate**

# COMMAND ----------

checkpoint_path = f"/Volumes/spotify_cata/silver/general_checkpoints/{uuid.uuid4()}"
dbutils.fs.mkdirs(checkpoint_path)
df_date = spark.readStream.format('cloudFiles')\
                      .option('cloudFiles.format', 'parquet')\
                      .option('cloudFiles.schemaLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimDate/checkpoint')\
                      .option('schemaEvolutionMode', 'addNewColumns')\
                      .option("checkpointLocation", checkpoint_path)\
                      .load('abfss://bronze@datalakespotifyazureprj.dfs.core.windows.net/DimDate/')

# COMMAND ----------

df_date =  reusable().dropColumns(df_date, ['_rescued_data'])

# Use this if you want to create a table here in Unity Catalog as well as store the table's data to external location i.e on ADLS Gen 2
df_date.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimDate/checkpoint')\
        .trigger(once=True)\
        .option('path', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/DimDate/data')\
        .toTable('spotify_cata.silver.DimDate')

# COMMAND ----------

# MAGIC %md
# MAGIC ### **FactStream**

# COMMAND ----------

checkpoint_path = f"/Volumes/spotify_cata/silver/general_checkpoints/{uuid.uuid4()}"
dbutils.fs.mkdirs(checkpoint_path)
df_fact = spark.readStream.format('cloudFiles')\
                      .option('cloudFiles.format', 'parquet')\
                      .option('cloudFiles.schemaLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/FactStream/checkpoint')\
                      .option('schemaEvolutionMode', 'addNewColumns')\
                      .option("checkpointLocation", checkpoint_path)\
                      .load('abfss://bronze@datalakespotifyazureprj.dfs.core.windows.net/FactStream/')

# COMMAND ----------

df_fact =  reusable().dropColumns(df_fact, ['_rescued_data'])

# Use this if you want to create a table here in Unity Catalog as well as store the table's data to external location i.e on ADLS Gen 2
df_fact.writeStream.format('delta')\
        .outputMode('append')\
        .option('checkpointLocation', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/FactStream/checkpoint')\
        .trigger(once=True)\
        .option('path', 'abfss://silver@datalakespotifyazureprj.dfs.core.windows.net/FactStream/data')\
        .toTable('spotify_cata.silver.FactStream')