import pytest

from airflow.models import DagBag


def test_no_import_errors():
    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No import failures"


def test_reties_present():
    dag_bag = DagBag()
    for dag in dag_bag.dags:
        retires = dag_bag.dags[dag].default_args.get('retires', [])
        error_msg = 'Retries not set to 2 for DAG {id}'.format(id=dag)
        assert retires == 2, error_msg
