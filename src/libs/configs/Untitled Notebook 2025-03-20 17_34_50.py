# Databricks notebook source
from pyspark.sql import SparkSession
import os

def get_common_files_path(enviornment):
    files_path = f"../../../configs/{enviornment}"
    return f"file:{os.path.abspath(files_path)}"

def get_tables_to_extract(spark, source_system, enviornment):
    config_path = f"{get_common_files_path(enviornment)}/extract/objects_to_extract"
    df = spark.read.option("multiline", "true").json(config_path)
    return [
        TableToExtract(row["object_name"], row["landing_zone_url"], row["source_format"])
        for row in
        df.filter(df.source_system == source_system).collect()
    ]

    

# COMMAND ----------


