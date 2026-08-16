from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

arquivo = "/Volumes/anac/bronze/arquivos/ocorrencias.csv"

df = spark.read \
    .option("header", True) \
    .option("sep", ";") \
    .csv(arquivo)

df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "anac.bronze.ocorrencias"
    )