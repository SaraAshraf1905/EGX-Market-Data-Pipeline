from datetime import datetime
from airflow import DAG
from gold_metrics_tasks import create_gold_metrics_tasks

with DAG(
    dag_id="gold_metrics_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["gold", "dbt", "testing"],
) as dag:

    create_gold_metrics_tasks(dag)