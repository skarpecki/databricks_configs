from pyspark.sql import SparkSession
from textwrap import dedent

def create_catalog(
    catalog_name: str,
    container_name: str,
    account_name: str
):
    spark = SparkSession.getActiveSession()
    query = dedent(f"""
        CREATE CATALOG IF NOT EXISTS `{catalog_name}`
        MANAGED LOCATION 'abfss://{container_name}@{account_name}.dfs.core.windows.net/'""")
    spark.sql(query)

def drop_catalog(catalog: str):
    spark = SparkSession.getActiveSession()
    spark.sql(f"DROP CATALOG IF EXISTS `{catalog}`")

def create_external_location(
    name: str,
    container_name: str,
    account_name: str,
    storage_credential_name: str
):
    spark = SparkSession.getActiveSession()
    query = dedent(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{name}`
        URL 'abfss://{container_name}@{account_name}.dfs.core.windows.net/'
        WITH (STORAGE CREDENTIAL `{storage_credential_name}`)
        """)
    spark.sql(query)


def create_schema(
    schema_name: str,
    catalog_name: str,
    container_name: str,
    account_name: str
):
    spark = SparkSession.getActiveSession()
    query = dedent(f"""
        CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`
        MANAGED LOCATION 'abfss://{container_name}@{account_name}.dfs.core.windows.net/{schema_name}'
        """)
    spark.sql(query)
