# Databricks notebook source
dbutils.widgets.dropdown("Environment", "dev", ["dev", "prod"])
param_env = dbutils.widgets.get("Environment")

# COMMAND ----------

import os

files_path = f"../configs/{param_env}"
files_path = f"file:{os.path.abspath(files_path)}"

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load tables to extract

# COMMAND ----------

catalog_name = "config"
schema_name = "config"
table_name = "raw_objects"

df = (
    spark.read.option("multiline", "true").json(f"{files_path}/extract/objects_to_extract")
    .withColumns({
    "inserted_at": current_timestamp(),
    "last_modified": current_timestamp()}
))

dt = DeltaTable.forName(spark, f"{catalog_name}.{schema_name}.{table_name}")
tgt_columns = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}").columns

cols_to_set = {col: f"src.{col}" for col in tgt_columns if col != "last_modified"}

(
    dt.alias("tgt")
    .merge(df.alias("src"), "src.object_name = tgt.object_name")
    .whenMatchedUpdate(set=cols_to_set)
    .whenNotMatchedInsertAll()
    .whenNotMatchedBySourceDelete()
    .execute()
)
