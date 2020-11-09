import os
from bq_airflow_dag_generator import generate_airflow_dag_by_dot_path
import unittest


class TestDAGs(unittest.TestCase):
    def test_dags(self):
        dagpath = os.path.join(os.path.dirname(__file__), "test_data")
        os.environ["SQL_ROOT"] = dagpath
        for curDir, _, files in os.walk(dagpath):
            for file in files:
                if not file.endswith(".dot"):
                    continue
                dot_path = os.path.join(curDir, file)
                try:
                    generate_airflow_dag_by_dot_path(dot_path)
                except ValueError as e:
                    if "cycle" in dot_path:
                        continue
                    raise e
