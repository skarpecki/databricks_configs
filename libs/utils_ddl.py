from pyspark.sql import SparkSession

spark = SparkSession.getActiveSession()

def create_managed_catalog(catalog: str):
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

def drop_managed_catalog(catalog: str):
    spark.sql(f"DROP CATALOG IF EXISTS {catalog}")