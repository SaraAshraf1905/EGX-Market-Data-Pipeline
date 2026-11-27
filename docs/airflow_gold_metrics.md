# Airflow Gold and Metrics Tasks

## Implemented Tasks
- Task 5: merge_to_gold
- Task 6: run_dbt_models
- Task 7: test_dbt_models
- Task 8: export_metrics_csv

## Passed Validation
- Docker image build
- DAG parsing
- Import errors check
- Task dependencies
- dbt parse
- Git installation

## Configuration
- Mount the repository at /opt/airflow/project.
- Pass the configured .env to the container.
- Copy profiles.yml.example to profiles.yml inside dbt-transformations.
- Change DBT_SQL_SERVER to SQLSERVER_HOST in the local profile.
- Configure the SQL Server host, port, database and credentials.
- Provide compatible Silver Parquet files.

## Integration
Import create_gold_metrics_tasks from gold_metrics_tasks.

Inside the main DAG:
- merge, export = create_gold_metrics_tasks(dag)
- spark_clean >> merge

## Pending Tests
- SQL Server connectivity (local connection test failed).
- Silver to Gold execution.
- dbt run and dbt test.
- CSV export verification.
- End-to-end integration.

The original SQL schemas and business logic remain unchanged.