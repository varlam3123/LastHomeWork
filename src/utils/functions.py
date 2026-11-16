from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.catalog import Column


def get_table(df: DataFrame, start_date: str, end_date: str | None = None):
    return df.filter((df.CutoffDt <= end_date) & (df.CutoffDt >= start_date))


def get_money(col1: Column, col2: Column):
    return F.regexp_replace(col1, ",", ".").cast("double") * F.regexp_replace(col2, ",", ".").cast("double")


def save_table_postgresql(df: DataFrame, nameTable: str):
    connection_properties = {
        "user": "postgres",
        "password": "de-school",
        "driver": "org.postgresql.Driver",
    }
    df.repartition(1).write.jdbc(
        url="jdbc:postgresql://localhost:5432/postgres",
        mode='overwrite',
        table=f'SchemaBank.{nameTable}',
        properties=connection_properties
    )

def save_part_parquet(df: DataFrame, path: str, col_part: Column, numPartitions: int):
    return df.repartition(numPartitions, col_part) \
        .write \
        .mode('overwrite') \
        .partitionBy(col_part) \
        .parquet(path)
