from dataclasses import dataclass

@dataclass
class UnityCatalogObject:
    catalog_name: str
    schema_name: str
    object_name: str
    location: ""