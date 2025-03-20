# Databricks notebook source
dbutils.widgets.dropdown("drop_tables", "False", ["True", "False"], "Drop Tables")
param_drop_tables = dbutils.widgets.get("drop_tables") == "True"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create needed structures
# MAGIC

# COMMAND ----------

import sys
sys.path.append("../libs")
sys.path.append("../configs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequsities
# MAGIC - Created storage account ADLS Gen. 2
# MAGIC - Created container in this account
# MAGIC - Created storage credential for this account

# COMMAND ----------

from libs.utils.ddl import *
from libs.utils import *
from configs.base import BaseConfig

create_external_location(f"ext_{account_name}_{adb_container_name}", adb_container_name, account_name, storage_credential_name)
create_external_location(f"ext_{account_name}_{landing_container_name}", landing_container_name, account_name, storage_credential_name)
create_catalog(config_catalog_name, adb_container_name, account_name)
create_schema(config_schema, config_catalog_name, adb_container_name, account_name)

# COMMAND ----------

table_name = "raw_objects"
table_location = f"{get_adls_location(adb_container_name, account_name)}/{config_schema}/{table_name}"

uc_table = UnityCatalogTable(
    catalog_name=config_catalog_name,
    schema_name=config_schema,
    object_name=table_name,
    location=table_location,
    columns=[
        Column(column_name="object_name",       data_type="string",     comment="",     nullable=False),
        Column(column_name="source_system",     data_type="string",     comment="",     nullable=False),
        Column(column_name="landing_zone_url",  data_type="string",     comment="",     nullable=False),
        Column(column_name="source_format",     data_type="string",     comment="",     nullable=False),
        Column(column_name="inserted_at",       data_type="timestamp",  comment="When record was created",          nullable=False),
        Column(column_name="last_modified",     data_type="timestamp",  comment="When record was last modified",    nullable=False),
    ])

if param_drop_tables:
    drop_table_if_exists(dbutils, uc_table)

dt = create_table(uc_table)
dt.detail().display()



# COMMAND ----------

# MAGIC %md
# MAGIC Useful for row level security and column level security, as can be easily integrated into RLS/CLS functions in views definition
