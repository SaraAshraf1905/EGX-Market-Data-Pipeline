from airflow.operators.bash import BashOperator


def create_merge_to_gold_task(dag):

    return BashOperator(
        task_id="merge_to_gold",
        bash_command=(
            "python /opt/airflow/project/"
            "src/incremental_Load/load_merged.py"
        ),
        dag=dag,
    )
def create_run_dbt_models_task(dag):

    return BashOperator(
        task_id="run_dbt_models",
        bash_command=(
            "cd /opt/airflow/project/dbt-transformations "
            "&& dbt run --profiles-dir ."
        ),
        dag=dag,
    )
def create_test_dbt_models_task(dag):

    return BashOperator(
        task_id="test_dbt_models",
        bash_command=(
            "cd /opt/airflow/project/dbt-transformations "
            "&& dbt test --profiles-dir ."
        ),
        dag=dag,
    )
def create_export_metrics_csv_task(dag):

    return BashOperator(
        task_id="export_metrics_csv",
        bash_command=(
            "python /opt/airflow/project/"
            "src/export/export_metrics_csv.py"
        ),
        dag=dag,
    )
def create_gold_metrics_tasks(dag):

    merge = create_merge_to_gold_task(dag)
    run = create_run_dbt_models_task(dag)
    test = create_test_dbt_models_task(dag)
    export = create_export_metrics_csv_task(dag)

    merge >> run >> test >> export

    return merge, export

