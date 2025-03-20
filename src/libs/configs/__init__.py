from dataclasses import dataclass

@dataclass
class TableToExtract:
    table_name: str
    landing_zone_url: str
    file_format: str