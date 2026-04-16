# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM spotify_cata.gold.dimdate

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC -- where __END_AT is not null
# MAGIC
# MAGIC SELECT * FROM spotify_cata.gold.dimtrack
# MAGIC where track_id in (46,5)