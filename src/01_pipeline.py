# Databricks notebook source
dbutils.widgets.dropdown("config source", "table", ["table", "file"])
param_config_source = dbutils.widgets.get("config source")

# COMMAND ----------

import sys

sys.path.append("../")

if param_config_source == 'file':
    from libs.configs.files import get_tables_to_extract
elif param_config_source == 'table':
    from libs.configs.tables import get_tables_to_extract
else:
    raise AttributeError("Unknown parameter config source:")

# COMMAND ----------

# To avoid errors as parquets were generated with pandas
spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")

for table in get_tables_to_extract(spark, "AdventureWorks", "dev"):
    spark.read.load(table.landing_zone_url, format=table.file_format).display()
