from __future__ import absolute_import, unicode_literals

from arango import ArangoClient
from arango.batch import BatchJob
from arango.exceptions import (
    DocumentInsertError
)
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
db = arango_client.create_database(db_name)
col_name = generate_col_name(db)
col = db.create_collection(col_name)


def teardown_module(*_):
    arango_client.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


def test_batch_insert_context_manager_with_result():
    assert len(col) == 0
    with db.batch(return_result=True) as batch_db:
        batch_col = batch_db.collection(col_name)
        batch_job1 = batch_col.insert_one({'_key': '1', 'val': 1})
        batch_job2 = batch_col.insert_one({'_key': '2', 'val': 2})
        batch_job3 = batch_col.insert_one({'_key': '2', 'val': 3})

    assert len(col) == 2
    assert col['1']['val'] == 1
    assert col['2']['val'] == 2

    assert batch_job1.status == BatchJob.Status.DONE
    assert batch_job1.result()['_key'] == '1'
    assert batch_job1.exception() is None

    assert batch_job2.status == BatchJob.Status.DONE
    assert batch_job2.result()['_key'] == '2'
    assert batch_job2.exception() is None

    assert batch_job3.status == BatchJob.Status.ERROR
    assert batch_job3.result() is None
    assert isinstance(batch_job3.exception(), DocumentInsertError)


def test_batch_insert_context_manager_without_result():
    assert len(col) == 0
    with db.batch(return_result=False) as batch:
        batch_col = batch.collection(col_name)
        batch_job1 = batch_col.insert_one({'_key': '1', 'val': 1})
        batch_job2 = batch_col.insert_one({'_key': '2', 'val': 2})
        batch_job3 = batch_col.insert_one({'_key': '2', 'val': 3})

    assert len(col) == 2
    assert col['1']['val'] == 1
    assert col['2']['val'] == 2

    assert batch_job1 is None
    assert batch_job2 is None
    assert batch_job3 is None


def test_batch_insert_no_context_manager_with_result():
    assert len(col) == 0
    batch = db.batch(return_result=True)
    batch_col = batch.collection(col_name)
    batch_job1 = batch_col.insert_one({'_key': '1', 'val': 1})
    batch_job2 = batch_col.insert_one({'_key': '2', 'val': 2})
    batch_job3 = batch_col.insert_one({'_key': '2', 'val': 3})

    assert len(col) == 0
    assert batch_job1.status == BatchJob.Status.PENDING
    assert batch_job1.result() is None
    assert batch_job1.exception() is None

    assert batch_job2.status == BatchJob.Status.PENDING
    assert batch_job2.result() is None
    assert batch_job2.exception() is None

    assert batch_job3.status == BatchJob.Status.PENDING
    assert batch_job3.result() is None
    assert batch_job3.exception() is None

    batch.commit()
    assert len(col) == 2
    assert col['1']['val'] == 1
    assert col['2']['val'] == 2

    assert batch_job1.status == BatchJob.Status.DONE
    assert batch_job1.result()['_key'] == '1'
    assert batch_job1.exception() is None

    assert batch_job2.status == BatchJob.Status.DONE
    assert batch_job2.result()['_key'] == '2'
    assert batch_job2.exception() is None

    assert batch_job3.status == BatchJob.Status.ERROR
    assert batch_job3.result() is None
    assert isinstance(batch_job3.exception(), DocumentInsertError)


def test_batch_insert_no_context_manager_without_result():
    assert len(col) == 0
    batch = db.batch(return_result=False)
    batch_col = batch.collection(col_name)
    batch_job1 = batch_col.insert_one({'_key': '1', 'val': 1})
    batch_job2 = batch_col.insert_one({'_key': '2', 'val': 2})
    batch_job3 = batch_col.insert_one({'_key': '2', 'val': 3})

    assert batch_job1 is None
    assert batch_job2 is None
    assert batch_job3 is None

    batch.commit()
    assert len(col) == 2
    assert col['1']['val'] == 1
    assert col['2']['val'] == 2
