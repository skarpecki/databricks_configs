from .structures import TableToExtract
import os
from pathlib import Path

def get_common_files_path(enviornment):
    # Ensures correct path. Relative import would be from place of calling method.
    files_path = Path(__file__).parent.parent.parent.parent
    return f"file:{os.path.abspath(files_path)}/configs/{enviornment}"

def get_tables_to_extract(spark, source_system, enviornment):
    config_path = f"{get_common_files_path(enviornment)}/extract/objects_to_extract"
    df = spark.read.option("multiline", "true").json(config_path)
    return [
        TableToExtract(row["object_name"], row["landing_zone_url"], row["source_format"])
        for row in
        df.filter(df.source_system == source_system).collect()
    ]