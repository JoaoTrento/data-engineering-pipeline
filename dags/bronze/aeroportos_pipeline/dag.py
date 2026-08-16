from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path
import requests
import os
from src.extract.extract_dados import extract_dados_url
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

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

        caminho = '/opt/airflow/data/raw/aeroportos.csv'
        with open(caminho, "w", encoding="utf-8") as arquivo:
                    arquivo.write(response.text)

        return caminho

    @task
    def upload_databricks(caminho):
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")

        destino = "/Volumes/anac/bronze/arquivos/aeroportos.csv"

        with open(caminho, "rb") as arquivo:
            response = requests.put(
                f"{host}/api/2.0/fs/files{destino}",
                headers={
                    "Authorization": f"Bearer {token}"
                },
                data=arquivo
            )

        response.raise_for_status()

    arquivo = get_dados_aeroportos()
    upload = upload_databricks(arquivo)
    executar_spark = DatabricksRunNowOperator(
        task_id="bronze_aeroportos",
        job_id="1027765287305088",
    )

    arquivo >> upload >> executar_spark

aeroportos_pipeline_bronze()