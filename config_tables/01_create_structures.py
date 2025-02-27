# Databricks notebook source
# MAGIC %md
# MAGIC ## Create needed structures
# MAGIC

# COMMAND ----------

import sys
sys.path.append("../libs")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Prerequsities
# MAGIC - Created storage account ADLS Gen. 2
# MAGIC - Created container in this account
# MAGIC - Create storage credential for this account

# COMMAND ----------

from libs.utils.ddl import *

# Must exists
storage_credential_name = "unity-catalog-access-connector"
container_name = "data"
account_name = "skarpeckiadb"

# Will be created
catalog_name = "config"
schema_name = "config"

create_external_location(f"ext_{account_name}", container_name, account_name, storage_credential_name)
create_catalog(catalog_name, container_name, account_name)
create_schema(schema_name, catalog_name, container_name, account_name)
