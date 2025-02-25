# Databricks notebook source
# MAGIC %md
# MAGIC ## Create needed structures
# MAGIC

# COMMAND ----------

import sys
sys.path.append("../libs")

# COMMAND ----------

from utils_ddl import *

catalog_name = "config"
schema_name = "config"

create_managed_catalog(catalog_name)
