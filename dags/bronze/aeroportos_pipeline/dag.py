from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path

import pandas as pd
from io import StringIO

from src.extract.extract_dados import extract_dados_url

README = Path(__file__).with_name("dag.md").read_text(encoding="utf-8")

@dag(
    dag_id="aeroportos_pipeline_bronze",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=README
)
def aeroportos_pipeline_bronze():

    @task
    def get_dados_aeroportos():
        url = "https://ourairports.com/countries/BR/airports.csv"
        response = extract_dados_url(url)

        df = pd.read_csv(
            StringIO(response.text),
            sep=','
        )
    
        df.to_parquet(
            "/opt/airflow/data/raw/aeroportos.parquet",
            index=False
        )

    get_dados_aeroportos()

aeroportos_pipeline_bronze()