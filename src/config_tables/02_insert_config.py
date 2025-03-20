# Databricks notebook source
dbutils.widgets.dropdown("delete_rows", "False", ["True", "False"], "Delete rows")
param_delete_rows = dbutils.widgets.get("delete_rows") == "True"

# COMMAND ----------

import sys
sys.path.append("../libs")

from libs.utils.ddl import *
from libs.utils import *
from delta.tables import DeltaTable

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

container_name = "data"
account_name = "skarpeckiadb"

catalog_name = "config"
schema_name = "config"
table_name = "raw_objects"

table_location = f"{get_adls_location(container_name, account_name)}/{schema_name}/{table_name}"

land_url_base = get_adls_location("landing", account_name)
data_dict = [
    {"object_name": "address", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=Address", "source_format": "parquet"},
    {"object_name": "customer", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=Customer", "source_format": "parquet"},
    {"object_name": "customer_address", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=CustomerAddress", "source_format": "parquet"},
    {"object_name": "product", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=Product", "source_format": "parquet"},
    {"object_name": "product_category", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=ProductCategory", "source_format": "parquet"},
    {"object_name": "product_description", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=ProductDescription", "source_format": "parquet"},
    {"object_name": "product_model", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=ProductModel", "source_format": "parquet"},
    {"object_name": "product_model_product_description", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=ProductModelProductDescription", "source_format": "parquet"},
    {"object_name": "sales_order_detail", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=SalesOrderDetail", "source_format": "parquet"},
    {"object_name": "sales_order_header", "source_system": "AdventureWorks", "landing_zone_url": f"{land_url_base}/catalog=AdventureWorksLT2022/schema=SalesLT/object=SalesOrderHeader", "source_format": "parquet"},
]

df = spark.createDataFrame(data_dict).withColumns({
    "inserted_at": current_timestamp(),
    "last_modified": current_timestamp() 
})

dt = DeltaTable.forName(spark, f"{catalog_name}.{schema_name}.{table_name}")
tgt_columns = spark.read.table(f"{catalog_name}.{schema_name}.{table_name}").columns

cols_to_set = {col: f"src.{col}" for col in tgt_columns if col != "last_modified"}

dt_merge = (
    dt.alias("tgt")
    .merge(df.alias("src"), "src.object_name = tgt.object_name")
    .whenMatchedUpdate(set=cols_to_set)
    .whenNotMatchedInsertAll()
)

if param_delete_rows:
    dt_merge = dt_merge.whenNotMatchedBySourceDelete()

dt = dt_merge.execute()

