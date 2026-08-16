from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np

README = Path(__file__).with_name("dag.md").read_text(encoding="utf-8")

@dag(
    dag_id="aeroportos_pipeline_silver",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=README
)
def aeroportos_pipeline_silver():

    @task()
    def normaliza_nome_colunas():
        df = pd.read_parquet('/opt/airflow/data/raw/aeroportos.parquet')

        df = df[['ident', 'type', 'name', 'country_name', 'local_region', 'municipality']].rename(columns={
            'ident': 'icao_id',
            'type': 'categoria', 
            'name': 'aeroporto', 
            'country_name': 'pais', 
            'local_region': 'estado', 
            'municipality': 'cidade'
        })

        df.to_parquet('/opt/airflow/data/tmp/aeroportos_silver_transicao.parquet')

    @task()
    def trata_valores_categoria():
        df = pd.read_parquet('/opt/airflow/data/tmp/aeroportos_silver_transicao.parquet')

        df['categoria'] = df['categoria'].replace({
            'small_airport': 'pequeno',
            'medium_airport': 'médio',
            'large_airport': 'grande',
            'heliport': 'heliponto',
            'closed': 'fechado'
        })

        df.to_parquet('/opt/airflow/data/processed/aeroportos.parquet')

    normaliza_nome_colunas() >> trata_valores_categoria()

aeroportos_pipeline_silver()