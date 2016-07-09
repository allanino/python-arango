from __future__ import absolute_import, unicode_literals

import pytest

from arango import ArangoClient
from arango.collection import Collection
from arango.graph import Graph
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    generate_graph_name
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
db = arango_client.create_database(db_name)
col_name_1 = generate_col_name(db)
col_name_2 = ''
db.create_collection(col_name_1)
graph_name = generate_graph_name(db)
db.create_graph(graph_name)


def teardown_module(*_):
    arango_client.drop_database(db_name, ignore_missing=True)


def test_properties():
    assert db.name == db_name
    assert repr(db) == '<ArangoDB database "{}">'.format(db_name)


@pytest.mark.order1
def test_options():
    options = db.properties()
    assert 'id' in options
    assert 'path' in options
    assert options['system'] == False
    assert options['name'] == db_name


@pytest.mark.order2
def test_list_collections():
    cols = db.list_collections()
    assert all(c == col_name_1 or c.startswith('_') for c in cols)


@pytest.mark.order3
def test_get_collection():
    for col in [db.collection(col_name_1), db[col_name_1]]:
        assert isinstance(col, Collection)
        assert col.name == col_name_1


@pytest.mark.order4
def test_create_collection():
    global col_name_2

    # Test create duplicate collection
    with pytest.raises(CollectionCreateError):
        db.create_collection(col_name_1)

    # Test create collection with parameters
    col_name_2 = generate_col_name(db)
    col = db.create_collection(
        name=col_name_2,
        sync=True,
        compact=False,
        journal_size=7774208,
        system=False,
        volatile=False,
        key_generator="autoincrement",
        user_keys=False,
        key_increment=9,
        key_offset=100,
        edge=True,
        shard_count=2,
        shard_fields=["test_attr"]
    )
    options = col.properties()
    assert 'id' in options
    assert options['name'] == col_name_2
    assert options['sync'] == True
    assert options['compact'] == False
    assert options['journal_size'] == 7774208
    assert options['system'] == False
    assert options['volatile'] == False
    assert options['edge'] == True
    assert options['keygen'] == 'autoincrement'
    assert options['user_keys'] == False
    assert options['key_increment'] == 9
    assert options['key_offset'] == 100


@pytest.mark.order5
def test_drop_collection():
    # Test drop collection
    result = db.delete_collection(col_name_2)
    assert result is True
    assert col_name_2 not in db.list_collections()

    # Test drop missing collection
    with pytest.raises(CollectionDeleteError):
        db.delete_collection(col_name_2)

    # Test drop missing collection (ignore_missing)
    result = db.delete_collection(col_name_2, ignore_missing=True)
    assert result is False


@pytest.mark.order6
def test_list_graphs():
    assert db.list_graphs() == [graph_name]


@pytest.mark.order7
def test_get_graph():
    graph = db.graph(graph_name)
    assert isinstance(graph, Graph)
    assert graph.name == graph_name


@pytest.mark.order8
def test_create_graph():
    # Test create duplicate graph
    with pytest.raises(GraphCreateError):
        db.create_graph(graph_name)

    new_graph_name = generate_graph_name(db)
    db.create_graph(new_graph_name)
    assert new_graph_name in db.list_graphs()


@pytest.mark.order9
def test_drop_graph():
    # Test drop graph
    result = db.delete_graph(graph_name)
    assert result is True
    assert graph_name not in db.list_graphs()

    # Test drop missing graph
    with pytest.raises(GraphDeleteError):
        db.delete_graph(graph_name)

    # Test drop missing graph (ignore_missing)
    result = db.delete_graph(graph_name, ignore_missing=True)
    assert result is False
