from .common import _get_tables_to_extract

def get_tables_to_extract(spark, source_system, environment):
    config_table = "config.config.raw_objects"
    df = spark.read.table(config_table)
    return _get_tables_to_extract(df, source_system)