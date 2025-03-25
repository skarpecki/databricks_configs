from .structures import TableToExtract

def _get_tables_to_extract(df, source_system):
    return [
        TableToExtract(row["object_name"], row["landing_zone_url"], row["source_format"])
        for row in
        df.filter(df.source_system == source_system).collect()
    ]