import os
from ..src.bq_airflow_dag_generator import generate_dag_by_dot_path
from airflow.models import DagBag
import unittest


class TestHelloWorldDAG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.dagbag = DagBag()
        cls.dags = []

    def test_dags(self):
        os.environ["SQL_ROOT"] = "/Users/matts966/alphasql/"
        dagpath = os.path.join(os.path.dirname(__file__), "test_data")
        for dot_path in os.listdir(dagpath):
            dot_full_path = os.path.join(dagpath, dot_path)
            self.dags.append(generate_dag_by_dot_path(dot_full_path))
        for dag in self.dags:
            self.assertDictEqual(self.dagbag.import_errors, {})
            self.assertIsNotNone(dag)
            self.assertEqual(len(dag.tasks), 1)
