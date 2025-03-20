from . import TableToExtract
from pyspark.sql import SparkSession

def get_tables_to_extract(source_system):
    config_table = "config.config.raw_objects"
    spark = SparkSession.getActiveSession()
    df = spark.read.table(config_table)
    return [
        TableToExtract(row["object_name"], row["landing_zone_url"], row["source_format"])
        for row in
        df.filter(df.source_system == source_system).collect()
    ]