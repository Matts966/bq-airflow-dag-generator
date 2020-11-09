# bq-airflow-dag-generator

Utility package to generate Airflow DAG from DOT language to execute BigQuery efficiently mainly for [AlphaSQL](https://github.com/Matts966/alphasql).

## Install

```bash
pip install bq-airflow-dag-generator
```

## Usage

```python
# You can set SQL_ROOT if your SQL file paths in dag.dot are not on current directory.
os.environ["SQL_ROOT"] = "/path/to/sql/root"
dagpath = "/path/to/dag.dot"
dag = generate_airflow_dag_by_dot_path(dagpath)
```

You can add tasks to existing DAG like

```python
dagpath = "/path/to/dag.dot"
existing_airflow_dag
generate_airflow_dag_by_dot_path(dagpath, dag=existing_airflow_dag)
```

You can pass how to create Aiflow tasks like

```python
def gen_task(sql_file_path, dag):
    sql_root = os.environ.get("SQL_ROOT")
    sql_file_path = os.path.join(sql_root, sql_file_path) if sql_root else sql_file_path
    with open(sql_file_path, "r") as f:
        query = f.read()
        task = PythonOperator(
            task_id=sql_file_path.replace("/", ""),
            python_callable=get_bigquery_callable(query),
            dag=dag,
        )
        task.doc = f"""\
# BigQuery Task Documentation: {sql_file_path}
This is automatically generated.
Query:
{query}
"""
    return task

dagpath = "/path/to/dag.dot"
generate_airflow_dag_by_dot_path(dagpath, get_task_by_sql_path_and_dag=gen_task)
```

## Test

```bash
python -m unittest tests.test_dags
```
