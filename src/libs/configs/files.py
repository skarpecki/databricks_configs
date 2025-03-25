from .common import _get_tables_to_extract
import os
from pathlib import Path

def get_common_files_path(enviornment):
    # Ensures correct path. Relative import would be from place of calling method.
    files_path = Path(os.path.abspath(__file__)).parent.parent.parent.parent
    return f"file:{files_path}/configs/{enviornment}"

def get_tables_to_extract(spark, source_system, enviornment):
    config_path = f"{get_common_files_path(enviornment)}/extract/objects_to_extract"
    df = spark.read.option("multiline", "true").json(config_path)
    return _get_tables_to_extract(df, source_system)