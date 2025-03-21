from dataclasses import dataclass
from . import files
from . import tables

def get_tables_to_extract(config_src, spark, source_system, env):
    if config_src == 'file':
        return files.get_tables_to_extract(spark, source_system, env)
    elif config_src == 'table':
        return tables.get_tables_to_extract(spark, source_system, env)
    else:
        raise AttributeError(f"Unknown parameter config source: {config_src}")
