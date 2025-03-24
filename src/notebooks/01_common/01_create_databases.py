# Databricks notebook source
# MAGIC %run "./00_config_path"

# COMMAND ----------

from utils.ddl import *
from utils import *

container_name = "data"
account_name = "skarpeckiadb"
catalog_name = "bronze"
schemas = ["adventure_works_1", "adventure_works_2", "adventure_works_3"]

create_catalog(catalog_name, container_name, account_name)

for schema_name in schemas:
    create_schema(schema_name, catalog_name, container_name, account_name)
