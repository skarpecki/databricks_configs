# Databricks notebook source
# MAGIC %run "./00_config_path"

# COMMAND ----------

dbutils.widgets.dropdown("config source", "table", ["table", "file"])
param_config_source = dbutils.widgets.get("config source")

# COMMAND ----------


from libs.configs import get_tables_to_extract

# COMMAND ----------

# To avoid errors as parquets were generated with pandas
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

for table in get_tables_to_extract(param_config_source, spark, "AdventureWorks", "dev"):
    spark.read.load(table.landing_zone_url, format=table.file_format).display()

# COMMAND ----------

param_config_source
