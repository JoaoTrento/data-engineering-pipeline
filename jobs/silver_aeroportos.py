from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

spark = SparkSession.builder.getOrCreate()

def get_dados_aeroportos_bronze():
    bronze = spark.table("anac.bronze.aeroportos")

    silver = bronze.select(
        "ident",
        "type",
        "name",
        "country_name",
        "local_region",
        "municipality"
    )

    return silver

def traduz_colunas_valores_aeroportos(silver: DataFrame):
    silver = silver.withColumnRenamed("ident", "icao_id") \
        .withColumnRenamed("type", "categoria") \
        .withColumnRenamed("name", "aeroporto") \
        .withColumnRenamed("country_name", "pais") \
        .withColumnRenamed("local_region", "estado") \
        .withColumnRenamed("municipality", "cidade")

    silver = silver.replace(
        {
            "small_airport": "pequeno",
            "medium_airport": "médio",
            "large_airport": "grande",
            "heliport": "heliponto",
            "closed": "fechado"
        },
        subset=["categoria"]
    )

    return silver

def salva_aeroportos_silver(silver: DataFrame):
    silver.write \
        .format("delta") \
        .mode("overwrite") \
        .saveAsTable(
            "anac.silver.aeroportos"
    )