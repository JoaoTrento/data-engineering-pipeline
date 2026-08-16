from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import countDistinct

spark = SparkSession.builder.getOrCreate()

bronze = spark.table("anac.bronze.ocorrencias")

silver = bronze.toDF(
    *[
        coluna.lower()
        .strip()
        .replace(" ", "_")
        for coluna in bronze.columns
    ]
)

silver = silver.select(
    'numero_da_ocorrencia', 'operador_padronizado', 'classificacao_da_ocorrencia', 'data_da_ocorrencia',
    'hora_da_ocorrencia', 'municipio', 'uf', 'regiao', 'descricao_do_tipo', 'icao', 'latitude', 'longitude',
    'tipo_de_aerodromo', 'matricula', 'categoria_da_aeronave', 'operador', 'tipo_de_ocorrencia', 
    'fase_da_operacao', 'operacao', 'danos_a_aeronave','aerodromo_de_destino', 'aerodromo_de_origem',
    'lesoes_fatais_tripulantes', 'lesoes_fatais_passageiros', 'lesoes_fatais_terceiros', 'lesoes_graves_tripulantes',
    'lesoes_graves_passageiros', 'lesoes_graves_terceiros', 'lesoes_leves_tripulantes', 'lesoes_leves_passageiros',
    'lesoes_leves_terceiros', 'ilesos_tripulantes', 'ilesos_passageiros', 'lesoes_desconhecidas_tripulantes',
    'lesoes_desconhecidas_passageiros', 'lesoes_desconhecidas_terceiros', 'modelo', 'pmd', 'numero_de_assentos',
    'nome_do_fabricante', 'psso'
)

aeroportos = spark.table("anac.silver.aeroportos").select("icao_id", "categoria", "aeroporto")

silver = silver.join(
    aeroportos,
    silver.icao == aeroportos.icao_id,
    "left"
)
silver = silver \
    .withColumnRenamed("categoria", "categoria_icao") \
    .withColumnRenamed("aeroporto", "aeroporto_icao") \
    .drop("icao_id")

silver = silver.join(
    aeroportos,
    silver.aerodromo_de_destino == aeroportos.icao_id,
    "left"
)
silver = silver \
    .withColumnRenamed("categoria", "categoria_destino") \
    .withColumnRenamed("aeroporto", "aeroporto_destino") \
    .drop("icao_id")

silver = silver.join(
    aeroportos,
    silver.aerodromo_de_origem == aeroportos.icao_id,
    "left"
)
silver = silver \
    .withColumnRenamed("categoria", "categoria_origem") \
    .withColumnRenamed("aeroporto", "aeroporto_origem") \
    .drop("icao_id")

qtd_aeronaves = (
    silver
    .groupBy("numero_da_ocorrencia")
    .agg(
        countDistinct("matricula")
        .alias("qtd_aeronaves_envolvidas")
    )
)
silver = silver.join(
    qtd_aeronaves,
    on="numero_da_ocorrencia",
    how="left"
)

silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "anac.silver.ocorrencias"
    )