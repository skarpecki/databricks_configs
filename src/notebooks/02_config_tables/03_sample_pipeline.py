# Databricks notebook source
# MAGIC %run "./00_config_path"

# COMMAND ----------

from configs.tables import get_tables_to_extract

# COMMAND ----------

# To avoid errors as parquets were generated with pandas
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

for table in get_tables_to_extract(spark, "AdventureWorks", "dev"):
    spark.read.load(table.landing_zone_url, format=table.file_format).display()
