from .structures import TableToExtract

def get_tables_to_extract(spark, source_system, environment):
    config_table = "config.config.raw_objects"
    df = spark.read.table(config_table)
    return [
        TableToExtract(row["object_name"], row["landing_zone_url"], row["source_format"])
        for row in
        df.filter(df.source_system == source_system).collect()
    ]