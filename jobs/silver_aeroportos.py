from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

bronze = spark.table("anac.bronze.aeroportos")

silver = bronze.select(
    "ident",
    "type",
    "name",
    "country_name",
    "local_region",
    "municipality"
)

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

silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "anac.silver.aeroportos"
    )