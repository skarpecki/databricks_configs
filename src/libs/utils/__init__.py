from dataclasses import dataclass
from pyspark.sql import SparkSession, Row

def get_adls_location(container_name, account_name):
    return f"abfss://{container_name}@{account_name}.dfs.core.windows.net/"

@dataclass
class Column:
    column_name: str
    data_type: str
    comment: str
    nullable: bool

    @classmethod
    def from_row(cls, row: Row):
        return Column(row.column_name, row.nullable, row.data_type, row.comment)

@dataclass
class UnityCatalogObject:
    catalog_name: str
    schema_name: str
    object_name: str
    location: ""

    def get_full_object_name(self):
        return f"`{self.catalog_name}`.`{self.schema_name}`.`{self.object_name}`"

@dataclass
class UnityCatalogTable(UnityCatalogObject):
    columns: [Column]

    def set_columns_from_inf_schema(self, spark: SparkSession) -> None:
        query = f"""
            SELECT
                column_name,
                CASE 
                    WHEN is_nullable = 'NO' THEN FALSE
                    ELSE TRUE
                END AS nullable,
                data_type,
                comment
            FROM
                system.information_schema.columns
            WHERE
                table_catalog = '{self.catalog_name}'
                AND
                table_schema = '{self.schema_name}'
                AND
                table_name = '{self.object_name}'
        """
        df = spark.sql(query)
        self.columns = df.rdd.map(lambda row: Column.from_row(row)).collect()

@dataclass
class TableToCreate:
    UnityCatalogObject
    columns: [Column]