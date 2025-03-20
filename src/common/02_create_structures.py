# Databricks notebook source
dbutils.widgets.text("source_system", "AdventureWorks", "Source system")
param_source_system = dbutils.widgets.get("source_system")

# COMMAND ----------

import sys
sys.path.append("../libs")

from libs.configs.tables import get_tables_to_extract
from libs.utils import get_adls_location

# COMMAND ----------

account_name = "skarpeckiadb"
container_name = "data"
catalog_name = "bronze"
schemas = ["adventure_works_1", "adventure_works_2", "adventure_works_3"]

for schema in schemas:
    tbls_to_extract = get_tables_to_extract(param_source_system)
    spark.conf.set("spark.sql.legacy.parquet.nanosAsLong", "true")
    for tbl in tbls_to_extract:
        tbl_location = f"{get_adls_location(container_name, account_name)}/{schema}/{tbl.table_name}"
        df = spark.read.parquet(tbl.landing_zone_url)
        (
            df.limit(0)
            .write
            .mode("overwrite")
            .saveAsTable(f"{catalog_name}.{schema}.{tbl.table_name}")
        )
