from __future__ import absolute_import, unicode_literals

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
def test_batch_insert():
    assert len(col) == 0
    with database.batch(return_result=True) as db:
        db.collection(col_name).properties()
        db.collection(col_name).revision()
        db.collection(col_name).rotate()
        db.collection(col_name).set_properties(sync=True)
        db.collection(col_name).insert({'_key': '1', 'val': 1})
        db.collection(col_name).insert({'_key': '2', 'val': 2})
        db.collection(col_name).insert({'_key': '3', 'val': 3})

    assert len(db.result()) == 7
    assert len(col) == 3
