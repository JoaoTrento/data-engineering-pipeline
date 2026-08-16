from airflow.sdk import dag, task
from datetime import datetime
from pathlib import Path
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

README = Path(__file__).with_name("dag.md").read_text(encoding="utf-8")

@dag(
    dag_id="ocorrencias_pipeline_silver",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    doc_md=README
)
def ocorrencias_pipeline_silver():

    executar_spark = DatabricksRunNowOperator(
        task_id="silver_ocorrencias",
        job_id="660989119438970",
    )

    executar_spark

ocorrencias_pipeline_silver()