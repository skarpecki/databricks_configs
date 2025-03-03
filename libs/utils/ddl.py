from pyspark.sql import SparkSession
from textwrap import dedent
from . import UnityCatalogTable, get_adls_location
from delta.tables import DeltaTable

def create_catalog(
    catalog_name: str,
    container_name: str,
    account_name: str
) -> None:
    spark = SparkSession.getActiveSession()
    query = dedent(f"""
        CREATE CATALOG IF NOT EXISTS `{catalog_name}`
        MANAGED LOCATION '{get_adls_location(container_name, account_name)}'""")
    spark.sql(query)

def drop_catalog(
    catalog: str
) -> None:
    spark = SparkSession.getActiveSession()
    spark.sql(f"DROP CATALOG IF EXISTS `{catalog}`")

def create_external_location(
    name: str,
    container_name: str,
    account_name: str,
    storage_credential_name: str
) -> None:
    spark = SparkSession.getActiveSession()
    query = dedent(f"""
        CREATE EXTERNAL LOCATION IF NOT EXISTS `{name}`
        URL '{get_adls_location(container_name, account_name)}'
        WITH (STORAGE CREDENTIAL `{storage_credential_name}`)
        """)
    spark.sql(query)


def create_schema(
    schema_name: str,
    catalog_name: str,
    container_name: str,
    account_name: str
) -> None:
    spark = SparkSession.getActiveSession()
    query = dedent(f"""
        CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`
        MANAGED LOCATION '{get_adls_location(container_name, account_name)}'
        """)
    spark.sql(query)


def create_table(
    uc_table: UnityCatalogTable
) -> DeltaTable:
    spark = SparkSession.getActiveSession()
    dt = DeltaTable.createIfNotExists(spark).tableName(uc_table.get_full_object_name())
    for c in uc_table.columns:
        dt = dt.addColumn(c.column_name, dataType=c.data_type, comment=c.comment, nullable=c.nullable)
    dt = dt.location(uc_table.location)
    return dt.execute()

def drop_table_if_exists(
    dbutils,
    uc_table: UnityCatalogTable
) -> DeltaTable:
    spark = SparkSession.getActiveSession()
    spark.sql(f"DROP TABLE IF EXISTS {uc_table.get_full_object_name()}")
    dbutils.fs.rm(uc_table.location, recurse=True)
    