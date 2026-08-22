from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()

arquivo = "/Volumes/anac/bronze/arquivos/ocorrencias.csv"

def get_ocorrencias():
    df = spark.read \
        .option("header", True) \
        .option("sep", ";") \
        .csv(arquivo)

    return df

def salva_ocorrencias_databricks(df: DataFrame):
    df.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            "anac.bronze.ocorrencias"
        )