from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

README = Path(__file__).with_name("dag.md").read_text(encoding="utf-8")

@dag(
    dag_id="ocorrencias_pipeline_silver",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        catchup=False,
        doc_md=README
)
def ocorrencias_pipeline_silver():

    @task
    def normaliza_nome_colunas():
        df = pd.read_parquet('/opt/airflow/data/raw/ocorrencias.parquet')
        print(df.dtypes)

        df.columns = (
            df.columns
            .str.lower()
            .str.strip()
            .str.replace(" ", "_")
        )

        df = df[['numero_da_ocorrencia', 'operador_padronizado', 'classificacao_da_ocorrencia', 'data_da_ocorrencia',
                 'hora_da_ocorrencia', 'municipio', 'uf', 'regiao', 'descricao_do_tipo', 'icao', 'latitude', 'longitude',
                 'tipo_de_aerodromo', 'matricula', 'categoria_da_aeronave', 'operador', 'tipo_de_ocorrencia', 
                 'fase_da_operacao', 'operacao', 'danos_a_aeronave','aerodromo_de_destino', 'aerodromo_de_origem',
                 'lesoes_fatais_tripulantes', 'lesoes_fatais_passageiros', 'lesoes_fatais_terceiros', 'lesoes_graves_tripulantes',
                 'lesoes_graves_passageiros', 'lesoes_graves_terceiros', 'lesoes_leves_tripulantes', 'lesoes_leves_passageiros',
                 'lesoes_leves_terceiros', 'ilesos_tripulantes', 'ilesos_passageiros', 'lesoes_desconhecidas_tripulantes',
                 'lesoes_desconhecidas_passageiros', 'lesoes_desconhecidas_terceiros', 'modelo', 'pmd', 'numero_de_assentos',
                 'nome_do_fabricante', 'psso'
        ]]

        df.to_parquet('/opt/airflow/data/tmp/ocorrencias_silver_transicao.parquet')

    @task()
    def adiciona_informacoes_icao():
        df = pd.read_parquet('/opt/airflow/data/tmp/ocorrencias_silver_transicao.parquet')
        df_icao = pd.read_parquet('/opt/airflow/data/processed/aeroportos.parquet')
        df_icao = df_icao[['icao_id', 'categoria', 'aeroporto']]

        df = df.merge(df_icao, left_on='icao', right_on='icao_id', how='left')
        df = df.drop(columns='icao_id').rename(columns={'categoria': 'categoria_icao', 'aeroporto': 'aeroporto_icao'})

        df = df.merge(df_icao, left_on='aerodromo_de_destino', right_on='icao_id', how='left')
        df = df.drop(columns='icao_id').rename(columns={'categoria': 'categoria_destino', 'aeroporto': 'aeroporto_destino'})

        df = df.merge(df_icao, left_on='aerodromo_de_origem', right_on='icao_id', how='left')
        df = df.drop(columns='icao_id').rename(columns={'categoria': 'categoria_origem', 'aeroporto': 'aeroporto_origem'})

        df.to_parquet('/opt/airflow/data/tmp/ocorrencias_silver_transicao.parquet')

    @task()
    def cria_colunas_uteis():
        df = pd.read_parquet('/opt/airflow/data/tmp/ocorrencias_silver_transicao.parquet')

        df['qtd_aeronaves_envolvidas'] = df.groupby('numero_da_ocorrencia')['matricula'].transform('nunique')

        df.to_parquet('/opt/airflow/data/processed/ocorrencias.parquet')

    normaliza_nome_colunas() >> adiciona_informacoes_icao() >> cria_colunas_uteis()

ocorrencias_pipeline_silver()