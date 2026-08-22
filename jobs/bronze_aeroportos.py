from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()

arquivo = "/Volumes/anac/bronze/arquivos/aeroportos.csv"

def get_aeroportos():
    df = spark.read \
        .option("header", True) \
        .option("sep", ",") \
        .csv(arquivo)

    return df

def salva_aeroportos_databricks(df: DataFrame):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            "anac.bronze.aeroportos"
        )

df = get_aeroportos()
salva_aeroportos_databricks(df)