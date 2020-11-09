from datetime import timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from networkx.drawing.nx_pydot import read_dot
from networkx.algorithms.cycles import find_cycle
from networkx import NetworkXNoCycle

from google.cloud import bigquery

import os


def get_bigquery_callable(query):
    def callable():
        client = bigquery.Client()
        query_job = client.query(query)
        print(query_job.result())

    return callable

def default_get_airflow_task_by_sql_path_and_dag(sql_file_path, dag):
    sql_root = os.environ.get("SQL_ROOT")
    sql_file_path = os.path.join(sql_root, sql_file_path) if sql_root else sql_file_path
    with open(sql_file_path, "r") as f:
        query = f.read()
        task = PythonOperator(
            task_id=sql_file_path.replace("/", ""), python_callable=get_bigquery_callable(query), dag=dag,
        )
        task.doc = f"""\
# BigQuery Task Documentation: {sql_file_path}
This is automatically generated.
Query:
{query}
"""
    return task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trig
}

def generate_airflow_dag_by_digraph(
    graph, dag=None, get_task_by_sql_path_and_dag=default_get_airflow_task_by_sql_path_and_dag
):
    if dag is None:
        dag = DAG(
            "bigquery_dag_dot", default_args=default_args, description="A BigQuery DAG generated by dot language.",
        )
        dag.doc_md = """\
#### DAG Documentation
This DAG is generated from dot language to execute BigQuery SQLs.
For more information, see Task Instance Details.
"""
    try:
        if find_cycle(graph):
            raise ValueError("cycle found in graph, not a dag")
    except NetworkXNoCycle:
        pass
    tasks = {}
    for node in graph.nodes():
        file_path = eval(graph.nodes[node]["label"])
        if file_path in tasks:
            task = tasks[file_path]
        else:
            task = get_task_by_sql_path_and_dag(file_path, dag)
            tasks[file_path] = task
        for dependent_file_path in [eval(graph.nodes[edge[0]]["label"]) for edge in graph.in_edges(node)]:
            if dependent_file_path in tasks:
                dependent = tasks[dependent_file_path]
            else:
                dependent = get_task_by_sql_path_and_dag(dependent_file_path, dag)
                tasks[dependent_file_path] = dependent
            dependent >> task
    return dag

def generate_airflow_dag_by_dot_path(dot_path: str, dag=None, get_task_by_sql_path_and_dag=default_get_airflow_task_by_sql_path_and_dag):
    g = read_dot(dot_path)
    return generate_airflow_dag_by_digraph(g, dag)