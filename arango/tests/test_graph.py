from __future__ import absolute_import, unicode_literals

import pytest

from arango import ArangoClient
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    generate_graph_name,
    clean_keys
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
db = arango_client.create_database(db_name)
col_name = generate_col_name(db)
col = db.create_collection(col_name)
graph_name = generate_graph_name(db)
graph = db.create_graph(graph_name)


def teardown_module(*_):
    arango_client.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


@pytest.mark.order1
def test_properties():
    assert graph.name == graph_name
    assert repr(graph) == (
        "<ArangoDB graph '{}'>".format(graph_name)
    )


@pytest.mark.order2
def test_options():
    options = graph.properties()
    assert options['id'] == '_graphs/{}'.format(graph_name)
    assert options['name'] == graph_name
    assert options['revision'].isdigit()


@pytest.mark.order3
def test_create_vertex_collection():
    assert graph.get_vertex_collections() == []
    assert graph.create_vertex_collection('vcol1') == True
    assert graph.get_vertex_collections() == ['vcol1']
    assert graph.get_orphan_collections() == ['vcol1']
    assert 'vcol1' in db.list_collections()

    # Test create duplicate vertex collection
    with pytest.raises(VertexCollectionCreateError):
        graph.create_vertex_collection('vcol1')
    assert graph.get_vertex_collections() == ['vcol1']
    assert graph.get_orphan_collections() == ['vcol1']
    assert 'vcol1' in db.list_collections()

    assert graph.create_vertex_collection('vcol2') == True
    assert sorted(graph.get_vertex_collections()) == ['vcol1', 'vcol2']
    assert graph.get_orphan_collections() == ['vcol1', 'vcol2']
    assert 'vcol1' in db.list_collections()
    assert 'vcol2' in db.list_collections()


@pytest.mark.order4
def test_list_vertex_collections():
    assert graph.get_vertex_collections() == ['vcol1', 'vcol2']


@pytest.mark.order5
def test_delete_vertex_collection():
    assert sorted(graph.get_vertex_collections()) == ['vcol1', 'vcol2']
    assert graph.delete_vertex_collection('vcol1') == True
    assert graph.get_vertex_collections() == ['vcol2']
    assert 'vcol1' in db.list_collections()

    # Test delete missing vertex collection
    with pytest.raises(VertexCollectionDeleteError):
        graph.delete_vertex_collection('vcol1')

    assert graph.delete_vertex_collection('vcol2', purge=True) == True
    assert graph.get_vertex_collections() == []
    assert 'vcol1' in db.list_collections()
    assert 'vcol2' not in db.list_collections()


@pytest.mark.order6
def test_create_edge_definition():
    assert graph.get_edge_collections() == []
    assert graph.create_edge_collection('ecol1', [], []) == True
    assert graph.get_edge_collections() == [{
        'name': 'ecol1',
        'from_collections': [],
        'to_collections': []
    }]
    assert 'ecol1' in db.list_collections()

    # Test create duplicate edge definition
    with pytest.raises(EdgeDefinitionCreateError):
        assert graph.create_edge_collection('ecol1', [], [])
    assert graph.get_edge_collections() == [{
        'name': 'ecol1',
        'from_collections': [],
        'to_collections': []
    }]

    # Test create edge definition with existing vertex collection
    assert graph.create_vertex_collection('vcol1') == True
    assert graph.create_vertex_collection('vcol2') == True
    assert graph.create_edge_collection('ecol2', ['vcol1'], ['vcol2']) == True
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': [],
            'to_collections': []
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol2']
        }
    ]
    assert 'ecol2' in db.list_collections()

    # Test create edge definition with missing vertex collection
    assert graph.create_edge_collection('ecol3', ['vcol3'], ['vcol3']) == True
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': [],
            'to_collections': []
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol3',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol3']
        }
    ]
    assert 'vcol3' in graph.get_vertex_collections()
    assert 'vcol3' not in graph.get_orphan_collections()
    assert 'vcol3' in db.list_collections()
    assert 'ecol3' in db.list_collections()


@pytest.mark.order7
def test_list_edge_definitions():
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': [],
            'to_collections': []
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol3',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol3']
        }
    ]


@pytest.mark.order8
def test_replace_edge_definition():
    assert graph.replace_edge_collection(
        name='ecol1',
        from_collections=['vcol3'],
        to_collections=['vcol2']
    ) == True
    assert graph.get_orphan_collections() == ['vcol1']
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol3',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol3']
        }
    ]
    assert graph.replace_edge_collection(
        name='ecol2',
        from_collections=['vcol1'],
        to_collections=[]
    ) == True
    assert graph.get_orphan_collections() == []
    assert 'vcol3' not in graph.get_orphan_collections()
    assert graph.replace_edge_collection(
        name='ecol3',
        from_collections=['vcol4'],
        to_collections=['vcol4']
    ) == True
    with pytest.raises(EdgeDefinitionReplaceError):
        graph.replace_edge_collection(
            name='ecol4',
            from_collections=[],
            to_collections=['vcol1']
        )
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': []
        },
        {
            'name': 'ecol3',
            'from_collections': ['vcol4'],
            'to_collections': ['vcol4']
        }
    ]
    assert graph.get_orphan_collections() == []


@pytest.mark.order9
def test_delete_edge_definition():
    assert graph.delete_edge_collection('ecol3') == True
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': []
        }
    ]
    assert graph.get_orphan_collections() == ['vcol4']
    assert 'vcol4' in graph.get_vertex_collections()
    assert 'vcol4' in db.list_collections()
    assert 'ecol3' in db.list_collections()

    with pytest.raises(EdgeDefinitionDeleteError):
        graph.delete_edge_collection('ecol3')

    assert graph.delete_edge_collection('ecol1', purge=True) == True
    assert graph.get_edge_collections() == [
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': []
        }
    ]
    assert sorted(graph.get_orphan_collections()) == ['vcol2', 'vcol3', 'vcol4']
    assert 'ecol1' not in db.list_collections()
    assert 'ecol2' in db.list_collections()
    assert 'ecol3' in db.list_collections()


@pytest.mark.order10
def test_insert_vertex():
    vcol = graph.vertex_collection('vcol1')
    vertex1 = {'_key': '1', 'value': 1}
    vertex2 = {'_key': '2', 'value': 2}

    assert '1' not in vcol
    assert vcol.insert_one(vertex1)
    assert '1' in vcol
    assert len(vcol) == 1

    # Test insert vertex into missing collection
    with pytest.raises(VertexInsertError):
        assert vcol.insert_one('missing', vertex1)

    # Test insert duplicate vertex
    with pytest.raises(VertexInsertError):
        assert vcol.insert_one(vertex1)

    assert '2' not in vcol
    assert vcol.insert_one(vertex2)
    assert '2' in vcol
    assert len(vcol) == 2

    # Test insert duplicate vertex second time
    with pytest.raises(VertexInsertError):
        assert vcol.insert_one(vertex2)


@pytest.mark.order11
def test_get_vertex():
    vcol = graph.vertex_collection('vcol1')
    vertex1 = {'_key': '1', 'value': 1}
    vertex2 = {'_key': '2', 'value': 2}

    # Test get missing vertex
    assert vcol.fetch_by_key('0') is None

    # Test get existing vertex
    result = vcol.fetch_by_key('1')
    old_rev = result['_rev']
    assert clean_keys(result) == vertex1

    # Test get existing vertex with wrong revision
    new_rev = str(int(old_rev) + 1)
    with pytest.raises(VertexRevisionError):
        vcol.fetch_by_key('1', new_rev)

    assert clean_keys(vcol.fetch_by_key('2')) == vertex2


@pytest.mark.order12
def test_update_vertex():
    vcol = graph.vertex_collection('vcol1')
    assert 'foo' not in vcol.fetch_by_key('1')
    assert vcol.update_one('1', {'foo': 100})
    assert vcol.fetch_by_key('1')['foo'] == 100

    result = vcol.update_one('1', {'foo': 200, 'bar': 300})
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result
    assert '_rev' in result

    result = vcol.fetch_by_key('1')
    assert result['foo'] == 200
    assert result['bar'] == 300
    old_rev = result['_rev']

    result = vcol.update_one('1', {'bar': 500}, rev=old_rev)
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result
    assert '_rev' in result

    assert vcol.fetch_by_key('1')['bar'] == 500

    new_rev = str(int(old_rev) + 10)
    with pytest.raises(VertexRevisionError):
        vcol.update_one('1', {'bar': 600}, rev=new_rev)
    assert vcol.fetch_by_key('1')['bar'] == 500

    result = vcol.update_one('1', {'bar': 400}, sync=True)
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result
    assert vcol.fetch_by_key('1')['foo'] == 200
    assert vcol.fetch_by_key('1')['bar'] == 400

    result = vcol.update_one('1', {'bar': None}, keep_none=True)
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result
    assert vcol.fetch_by_key('1')['bar'] is None

    result = vcol.update_one('1', {'foo': None}, keep_none=False)
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result
    assert vcol.fetch_by_key('1')['bar'] is None
    assert 'foo' not in vcol.fetch_by_key('1')


@pytest.mark.order13
def test_replace_vertex():
    vcol = graph.vertex_collection('vcol1')

    # Check precondition
    assert 'bar' in vcol.fetch_by_key('1')
    assert 'value' in vcol.fetch_by_key('1')

    result = vcol.replace_one('1', {'baz': 100})
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result
    assert vcol.fetch_by_key('1')['baz'] == 100
    assert 'bar' not in vcol.fetch_by_key('1')
    assert 'value' not in vcol.fetch_by_key('1')

    result = vcol.replace_one('1', {'foo': 200, 'bar': 300})
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result

    result = vcol.fetch_by_key('1')
    assert clean_keys(result) == {'_key': '1', 'foo': 200, 'bar': 300}
    old_rev = result['_rev']

    result = vcol.replace_one('1', {'bar': 500}, rev=old_rev)
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result
    assert vcol.fetch_by_key('1')['bar'] == 500
    assert 'foo' not in vcol.fetch_by_key('1')

    new_rev = str(int(old_rev) + 10)
    with pytest.raises(VertexRevisionError):
        vcol.replace_one('1', {'bar': 600}, rev=new_rev)
    assert vcol.fetch_by_key('1')['bar'] == 500
    assert 'foo' not in vcol.fetch_by_key('1')

    result = vcol.replace_one('1', {'bar': 400, 'foo': 200}, sync=True)
    assert result['_id'] == 'vcol1/1'
    assert '_old_rev' in result and '_rev' in result
    assert vcol.fetch_by_key('1')['foo'] == 200
    assert vcol.fetch_by_key('1')['bar'] == 400


@pytest.mark.order14
def test_delete_vertex():
    vcol = graph.vertex_collection('vcol1')
    vcol.truncate()
    vcol.insert_one({'_key': '1', 'value': 1})
    vcol.insert_one({'_key': '2', 'value': 2})
    vcol.insert_one({'_key': '3', 'value': 3})

    # Test vertex delete
    assert vcol.clear('1') == True
    assert vcol.fetch_by_key('1') is None
    assert '1' not in vcol

    # Test vertex delete with sync
    assert vcol.clear('3', sync=True) == True
    assert vcol.fetch_by_key('3') is None
    assert '3' not in vcol

    # Test delete vertex with incorrect revision
    old_rev = vcol.fetch_by_key('2')['_rev']
    new_rev = str(int(old_rev) + 10)
    with pytest.raises(VertexRevisionError):
        vcol.clear('2', rev=new_rev)
    assert '2' in vcol

    # Test delete vertex from missing collection
    with pytest.raises(VertexDeleteError):
        graph.vertex_collection('missing').clear('1', ignore_missing=False)

    # Test delete missing vertex
    with pytest.raises(VertexDeleteError):
        vcol.clear('10', ignore_missing=False)


@pytest.mark.order15
def test_insert_edge():
    ecol = db.collection('ecol2')
    ecol.truncate()


@pytest.mark.order16
def test_get_edge():
    pass


@pytest.mark.order17
def test_update_edge():
    pass


@pytest.mark.order18
def test_replace_edge():
    pass


@pytest.mark.order19
def test_delete_edge():
    pass


@pytest.mark.order20
def test_traverse():
    pass
