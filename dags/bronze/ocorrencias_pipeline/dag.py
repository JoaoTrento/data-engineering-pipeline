from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path
import requests
import os
from src.extract.extract_dados import extract_dados_url
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

README = Path(__file__).with_name("dag.md").read_text(encoding="utf-8")

@dag(
    dag_id="ocorrencias_pipeline_bronze",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=README
)
def ocorrencias_pipeline_bronze():

    @task
    def get_dados_ocorrencias():
        url = "https://sistemas.anac.gov.br/dadosabertos/Seguranca%20Operacional/Ocorrencia/V_OCORRENCIA_AMPLA.csv"
        response = extract_dados_url(url)

        caminho = "/opt/airflow/data/raw/ocorrencias.csv"
        linhas = response.text.splitlines()

        with open(caminho, "w", encoding="utf-8") as arquivo:
            arquivo.write("\n".join(linhas[1:]))

        return caminho

    @task
    def upload_databricks(caminho):
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_TOKEN")

        destino = "/Volumes/anac/bronze/arquivos/ocorrencias.csv"

        with open(caminho, "rb") as arquivo:
            response = requests.put(
                f"{host}/api/2.0/fs/files{destino}",
                headers={
                    "Authorization": f"Bearer {token}"
                },
                data=arquivo
            )

        response.raise_for_status()
        
    arquivo = get_dados_ocorrencias()
    upload = upload_databricks(arquivo)
    executar_spark = DatabricksRunNowOperator(
        task_id="bronze_ocorrencias",
        job_id="309139740908451",
    )

    arquivo >> upload >> executar_spark

ocorrencias_pipeline_bronze()