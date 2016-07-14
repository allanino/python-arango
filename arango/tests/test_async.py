from __future__ import absolute_import, unicode_literals

from time import sleep

import pytest

from arango import ArangoClient
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
database = arango_client.create_database(db_name)
col_name = generate_col_name(database)
col = database.create_collection(col_name)


def teardown_module(*_):
    arango_client.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


@pytest.mark.order1
def test_async_insert():
    assert len(col) == 0
    async = database.async(return_result=True)
    job1 = async.collection(col_name).insert_one({'_key': '1', 'val': 1})
    job2 = async.collection(col_name).insert_one({'_key': '2', 'val': 2})
    job3 = async.collection(col_name).insert_one({'_key': '3', 'val': 3})

    sleep(1)
    print(job1.result())
    print(job2.result())
    print(job3.result())
    assert len(col) == 3
