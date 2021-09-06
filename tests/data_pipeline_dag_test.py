"""
This file contains test cases to test the basic configuration
and check for any import error in DAGS.
"""
from airflow.models import DagBag


def test_no_import_errors():
    """
    This method checks for any error in imports for DAG
    :return: Number of import errors or a message "No import failures"
    """
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No import failures"


def test_reties_present():
    """
    This test verifies the DAG retry configuration is set to 2
    :return: if DAG retry set to 2-> return True
            Else returns the message "Retries not set to 2 for DAG <id>"
    """
    dag_bag = DagBag()
    for dag in dag_bag.dags:
        retires = dag_bag.dags[dag].default_args.get('retires', [])
        error_msg = 'Retries not set to 2 for DAG {id}'.format(id=dag)
        assert retires == 2, error_msg
