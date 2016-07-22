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

# vertices in test vertex collection #1
vertex1 = {'_key': '1', 'value': 1}
vertex2 = {'_key': '2', 'value': 2}
vertex3 = {'_key': '3', 'value': 3}

# vertices in test vertex collection #2
vertex4 = {'_key': '4', 'value': 4}
vertex5 = {'_key': '5', 'value': 5}
vertex6 = {'_key': '6', 'value': 6}

# edges in test edge collection
edge1 = {'_key': '1', '_from': 'vcol1/1', '_to': 'vcol3/4'}  # valid
edge2 = {'_key': '2', '_from': 'vcol1/1', '_to': 'vcol3/5'}  # valid
edge3 = {'_key': '3', '_from': 'vcol3/6', '_to': 'vcol1/2'}  # invalid
edge4 = {'_key': '4', '_from': 'vcol1/8', '_to': 'vcol3/7'}  # missing

# new edges that will be updated/replaced to
edge5 = {'_key': '1', '_from': 'vcol1/1', '_to': 'vcol3/5'}  # valid
edge6 = {'_key': '1', '_from': 'vcol3/6', '_to': 'vcol1/2'}  # invalid
edge7 = {'_key': '1', '_from': 'vcol1/8', '_to': 'vcol3/7'}  # missing


def teardown_module(*_):
    arango_client.delete_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


@pytest.mark.order1
def test_properties():
    assert graph.name == graph_name
    assert repr(graph) == (
        "<ArangoDB graph '{}'>".format(graph_name)
    )
    properties = graph.properties()
    assert properties['id'] == '_graphs/{}'.format(graph_name)
    assert properties['name'] == graph_name
    assert properties['revision'].isdigit()


@pytest.mark.order2
def test_create_vertex_collection():
    # Check preconditions
    assert graph.vertex_collections() == []
    assert graph.create_vertex_collection('vcol1') is True
    assert graph.vertex_collections() == ['vcol1']
    assert graph.orphan_collections() == ['vcol1']
    assert 'vcol1' in db.collections()

    # Test create duplicate vertex collection
    with pytest.raises(VertexCollectionCreateError):
        graph.create_vertex_collection('vcol1')
    assert graph.vertex_collections() == ['vcol1']
    assert graph.orphan_collections() == ['vcol1']
    assert 'vcol1' in db.collections()

    # Test create valid vertex collection
    assert graph.create_vertex_collection('vcol2') is True
    assert sorted(graph.vertex_collections()) == ['vcol1', 'vcol2']
    assert graph.orphan_collections() == ['vcol1', 'vcol2']
    assert 'vcol1' in db.collections()
    assert 'vcol2' in db.collections()


@pytest.mark.order3
def test_list_vertex_collections():
    assert graph.vertex_collections() == ['vcol1', 'vcol2']


@pytest.mark.order4
def test_delete_vertex_collection():
    # Check preconditions
    assert sorted(graph.vertex_collections()) == ['vcol1', 'vcol2']
    assert graph.delete_vertex_collection('vcol1') is True
    assert graph.vertex_collections() == ['vcol2']
    assert 'vcol1' in db.collections()

    # Test delete missing vertex collection
    with pytest.raises(VertexCollectionDeleteError):
        graph.delete_vertex_collection('vcol1')

    # Test delete vertex collection with purge option
    assert graph.delete_vertex_collection('vcol2', purge=True) is True
    assert graph.vertex_collections() == []
    assert 'vcol1' in db.collections()
    assert 'vcol2' not in db.collections()


@pytest.mark.order5
def test_create_edge_definition():
    assert graph.edge_collections() == []
    assert graph.create_edge_collection('ecol1', [], []) is True
    assert graph.edge_collections() == [{
        'name': 'ecol1',
        'from_collections': [],
        'to_collections': []
    }]
    assert 'ecol1' in db.collections()

    # Test create duplicate edge definition
    with pytest.raises(EdgeDefinitionCreateError):
        assert graph.create_edge_collection('ecol1', [], [])
    assert graph.edge_collections() == [{
        'name': 'ecol1',
        'from_collections': [],
        'to_collections': []
    }]

    # Test create edge definition with existing vertex collection
    assert graph.create_vertex_collection('vcol1') is True
    assert graph.create_vertex_collection('vcol2') is True
    assert graph.create_edge_collection(
        name='ecol2',
        from_collections=['vcol1'],
        to_collections=['vcol2']
    ) is True
    assert graph.edge_collections() == [
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
    assert 'ecol2' in db.collections()

    # Test create edge definition with missing vertex collection
    assert graph.create_edge_collection(
        name='ecol3',
        from_collections=['vcol3'],
        to_collections=['vcol3']
    ) is True
    assert graph.edge_collections() == [
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
    assert 'vcol3' in graph.vertex_collections()
    assert 'vcol3' not in graph.orphan_collections()
    assert 'vcol3' in db.collections()
    assert 'ecol3' in db.collections()


@pytest.mark.order6
def test_list_edge_definitions():
    assert graph.edge_collections() == [
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


@pytest.mark.order7
def test_replace_edge_definition():
    assert graph.replace_edge_collection(
        name='ecol1',
        from_collections=['vcol3'],
        to_collections=['vcol2']
    ) is True
    assert graph.orphan_collections() == ['vcol1']
    assert graph.edge_collections() == [
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
        to_collections=['vcol3']
    ) is True
    assert graph.orphan_collections() == []
    assert 'vcol3' not in graph.orphan_collections()
    assert graph.replace_edge_collection(
        name='ecol3',
        from_collections=['vcol4'],
        to_collections=['vcol4']
    ) is True
    with pytest.raises(EdgeDefinitionReplaceError):
        graph.replace_edge_collection(
            name='ecol4',
            from_collections=[],
            to_collections=['vcol1']
        )
    assert graph.edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol3']
        },
        {
            'name': 'ecol3',
            'from_collections': ['vcol4'],
            'to_collections': ['vcol4']
        }
    ]
    assert graph.orphan_collections() == []


@pytest.mark.order8
def test_delete_edge_definition():
    assert graph.delete_edge_collection('ecol3') is True
    assert graph.edge_collections() == [
        {
            'name': 'ecol1',
            'from_collections': ['vcol3'],
            'to_collections': ['vcol2']
        },
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol3']
        }
    ]
    assert graph.orphan_collections() == ['vcol4']
    assert 'vcol4' in graph.vertex_collections()
    assert 'vcol4' in db.collections()
    assert 'ecol3' in db.collections()

    with pytest.raises(EdgeDefinitionDeleteError):
        graph.delete_edge_collection('ecol3')

    assert graph.delete_edge_collection('ecol1', purge=True) is True
    assert graph.edge_collections() == [
        {
            'name': 'ecol2',
            'from_collections': ['vcol1'],
            'to_collections': ['vcol3']
        }
    ]
    assert sorted(graph.orphan_collections()) == ['vcol2', 'vcol4']
    assert 'ecol1' not in db.collections()
    assert 'ecol2' in db.collections()
    assert 'ecol3' in db.collections()


@pytest.mark.order9
def test_insert_vertex():
    vcol = graph.vertex_collection('vcol1')

    # Check preconditions
    assert '1' not in vcol
    assert len(vcol) == 0

    # Test insert first vertex
    result = vcol.insert_one(vertex1)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_rev'].isdigit()
    assert '1' in vcol
    assert len(vcol) == 1
    assert vcol['1']['value'] == 1

    # Test insert vertex into missing collection
    with pytest.raises(DocumentInsertError):
        assert graph.v('missing').insert_one(vertex2)
    assert '2' not in vcol
    assert len(vcol) == 1

    # Test insert duplicate vertex
    with pytest.raises(DocumentInsertError):
        assert vcol.insert_one(vertex1)
    assert len(vcol) == 1

    # Test insert second vertex
    result = vcol.insert_one(vertex2)
    assert result['_id'] == 'vcol1/2'
    assert result['_key'] == '2'
    assert result['_rev'].isdigit()
    assert '2' in vcol
    assert len(vcol) == 2
    assert vcol['2']['value'] == 2

    # Test insert duplicate vertex second time
    with pytest.raises(DocumentInsertError):
        assert vcol.insert_one(vertex2)


@pytest.mark.order10
def test_fetch_vertex():
    vcol = graph.vertex_collection('vcol1')

    # Test get missing vertex
    assert vcol.fetch_by_key('0') is None

    # Test get existing vertex
    result = vcol.fetch_by_key('1')
    old_rev = result['_rev']
    assert clean_keys(result) == {'_key': '1', 'value': 1}

    # Test get existing vertex with wrong revision
    with pytest.raises(DocumentRevisionError):
        vcol.fetch_by_key('1', rev=str(int(old_rev) + 1))

    # Test get existing vertex again
    assert clean_keys(vcol.fetch_by_key('2')) == {'_key': '2', 'value': 2}


@pytest.mark.order11
def test_update_vertex():
    vcol = graph.vertex_collection('vcol1')

    # Test update vertex with a single field change
    assert 'foo' not in vcol.fetch_by_key('1')
    result = vcol.update_one({'_key': '1', 'foo': 100})
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert vcol['1']['foo'] == 100
    old_rev = vcol['1']['_rev']

    # Test update vertex with multiple field changes
    result = vcol.update_one({'_key': '1', 'foo': 200, 'bar': 300})
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert vcol['1']['foo'] == 200
    assert vcol['1']['bar'] == 300
    old_rev = result['_rev']

    # Test update vertex with correct revision
    result = vcol.update_one({'_key': '1', '_rev': old_rev, 'bar': 400})
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert vcol['1']['foo'] == 200
    assert vcol['1']['bar'] == 400
    old_rev = result['_rev']

    # Test update vertex with incorrect revision
    new_rev = str(int(old_rev) + 10)
    with pytest.raises(DocumentRevisionError):
        vcol.update_one({'_key': '1', '_rev': new_rev, 'bar': 500})
    assert vcol['1']['foo'] == 200
    assert vcol['1']['bar'] == 400

    # Test update vertex with sync option
    result = vcol.update_one({'_key': '1', 'bar': 500}, sync=True)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert vcol['1']['foo'] == 200
    assert vcol['1']['bar'] == 500
    old_rev = result['_rev']

    # Test update vertex with keep_none option
    result = vcol.update_one({'_key': '1', 'bar': None}, keep_none=True)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert vcol['1']['foo'] == 200
    assert vcol['1']['bar'] is None
    old_rev = result['_rev']

    # Test update vertex without keep_none option
    result = vcol.update_one({'_key': '1', 'foo': None}, keep_none=False)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert 'foo' not in vcol['1']
    assert vcol['1']['bar'] is None


@pytest.mark.order12
def test_replace_vertex():
    vcol = graph.vertex_collection('vcol1')

    # Check precondition
    assert 'bar' in vcol.fetch_by_key('1')
    assert 'value' in vcol.fetch_by_key('1')

    # Test replace vertex with a single field change
    result = vcol.replace_one({'_key': '1', 'baz': 100})
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert 'foo' not in vcol['1']
    assert 'bar' not in vcol['1']
    assert vcol['1']['baz'] == 100
    old_rev = result['_rev']

    # Test replace vertex with multiple field changes
    vertex = {'_key': '1', 'foo': 200, 'bar': 300}
    result = vcol.replace_one(vertex)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert clean_keys(vcol['1']) == vertex
    old_rev = result['_rev']

    # Test replace vertex with correct revision
    vertex = {'_key': '1', '_rev': old_rev, 'bar': 500}
    result = vcol.replace_one(vertex)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert clean_keys(vcol['1']) == clean_keys(vertex)
    old_rev = result['_rev']

    # Test replace vertex with incorrect revision
    new_rev = str(int(old_rev) + 10)
    vertex = {'_key': '1', '_rev': new_rev, 'bar': 600}
    with pytest.raises(DocumentRevisionError):
        vcol.replace_one(vertex)
    assert vcol['1']['bar'] == 500
    assert 'foo' not in vcol['1']

    # Test replace vertex with sync option
    vertex = {'_key': '1', 'bar': 400, 'foo': 200}
    result = vcol.replace_one(vertex, sync=True)
    assert result['_id'] == 'vcol1/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert vcol['1']['foo'] == 200
    assert vcol['1']['bar'] == 400


@pytest.mark.order13
def test_delete_vertex():
    vcol = graph.vertex_collection('vcol1')
    vcol.truncate()

    vcol.insert_one(vertex1)
    vcol.insert_one(vertex2)
    vcol.insert_one(vertex3)

    # Test vertex delete
    assert vcol.delete_one(vertex1) is True
    assert vcol['1'] is None
    assert '1' not in vcol

    # Test vertex delete with sync
    assert vcol.delete_one(vertex3, sync=True) is True
    assert vcol['3'] is None
    assert '3' not in vcol

    # Test delete vertex with incorrect revision
    old_rev = vcol['2']['_rev']
    vertex2['_rev'] = str(int(old_rev) + 10)
    with pytest.raises(DocumentRevisionError):
        vcol.delete_one(vertex2)
    assert '2' in vcol

    # Test delete vertex from missing collection
    with pytest.raises(DocumentDeleteError):
        graph.v('missing').delete_one(vertex1, ignore_missing=False)

    # Test delete missing vertex
    with pytest.raises(DocumentDeleteError):
        vcol.delete_one({'_key': '10'}, ignore_missing=False)

    # Test delete missing vertex while ignoring missing
    vcol.delete_one({'_key': '10'}, ignore_missing=True) is None


@pytest.mark.order14
def test_insert_edge():
    ecol = graph.edge_collection('ecol2')
    ecol.truncate()

    vcol1 = db.collection('vcol1')
    vcol1.truncate()
    vcol1.insert_many([vertex1, vertex2, vertex3])

    vcol3 = db.collection('vcol3')
    vcol3.truncate()
    vcol3.insert_many([vertex4, vertex5, vertex6])

    # Check preconditions
    assert '1' not in ecol
    assert len(ecol) == 0
    assert len(vcol1) == 3
    assert len(vcol3) == 3

    # Test insert first valid edge
    result = ecol.insert_one(edge1)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_rev'].isdigit()
    assert '1' in ecol
    assert len(ecol) == 1
    assert ecol['1']['_from'] == 'vcol1/1'
    assert ecol['1']['_to'] == 'vcol3/4'

    # Test insert valid edge into missing collection
    with pytest.raises(DocumentInsertError):
        assert graph.v('missing').insert_one(edge2)
    assert '2' not in ecol
    assert len(ecol) == 1

    # Test insert duplicate edge
    with pytest.raises(DocumentInsertError):
        assert ecol.insert_one(edge1)
    assert len(ecol) == 1

    # Test insert second valid edge
    result = ecol.insert_one(edge2)
    assert result['_id'] == 'ecol2/2'
    assert result['_key'] == '2'
    assert '2' in ecol
    assert len(ecol) == 2
    assert ecol['2']['_from'] == 'vcol1/1'
    assert ecol['2']['_to'] == 'vcol3/5'
    old_rev = result['_rev']

    # Test insert duplicate edge second time
    with pytest.raises(DocumentInsertError):
        assert ecol.insert_one(edge2)
    assert ecol['2']['_from'] == 'vcol1/1'
    assert ecol['2']['_to'] == 'vcol3/5'
    assert ecol['2']['_rev'] == old_rev

    # Test insert invalid edge (from and to mixed up)
    with pytest.raises(DocumentInsertError):
        ecol.insert_one(edge3)
    assert ecol['2']['_from'] == 'vcol1/1'
    assert ecol['2']['_to'] == 'vcol3/5'
    assert ecol['2']['_rev'] == old_rev

    # Test insert invalid edge (missing vertices)
    result = ecol.insert_one(edge4)
    assert result['_id'] == 'ecol2/4'
    assert result['_key'] == '4'
    assert result['_rev'].isdigit()
    assert '4' in ecol
    assert len(ecol) == 3
    assert ecol['4']['_from'] == 'vcol1/8'
    assert ecol['4']['_to'] == 'vcol3/7'
    assert len(vcol1) == 3
    assert len(vcol3) == 3
    assert '4' not in vcol1
    assert 'd' not in vcol3


@pytest.mark.order15
def test_fetch_edge():
    ecol = graph.edge_collection('ecol2')
    ecol.truncate()
    for edge in [edge1, edge2, edge4]:
        ecol.insert_one(edge)

    # Test get missing vertex
    assert ecol.fetch_by_key('0') is None

    # Test get existing vertex
    result = ecol.fetch_by_key('1')
    old_rev = result['_rev']
    assert clean_keys(result) == edge1

    # Test get existing vertex with wrong revision
    with pytest.raises(DocumentRevisionError):
        ecol.fetch_by_key('1', rev=str(int(old_rev) + 1))

    # Test get existing vertex again
    assert clean_keys(ecol.fetch_by_key('2')) == edge2


@pytest.mark.order16
def test_update_edge():
    ecol = graph.edge_collection('ecol2')
    ecol.truncate()
    ecol.insert_one(edge1)

    # Test update edge with a single field change
    assert 'foo' not in ecol.fetch_by_key('1')
    result = ecol.update_one({'_key': '1', 'foo': 100})
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert ecol['1']['foo'] == 100
    old_rev = ecol['1']['_rev']

    # Test update edge with multiple field changes
    result = ecol.update_one({'_key': '1', 'foo': 200, 'bar': 300})
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] == 300
    old_rev = result['_rev']

    # Test update edge with correct revision
    result = ecol.update_one({'_key': '1', '_rev': old_rev, 'bar': 400})
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] == 400
    old_rev = result['_rev']

    # Test update edge with incorrect revision
    new_rev = str(int(old_rev) + 10)
    with pytest.raises(DocumentRevisionError):
        ecol.update_one({'_key': '1', '_rev': new_rev, 'bar': 500})
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] == 400

    # Test update edge with sync option
    result = ecol.update_one({'_key': '1', 'bar': 500}, sync=True)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] == 500
    old_rev = result['_rev']

    # Test update edge without keep_none option
    result = ecol.update_one({'_key': '1', 'bar': None}, keep_none=True)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] is None
    old_rev = result['_rev']

    # Test update edge with keep_none option
    result = ecol.update_one({'_key': '1', 'foo': None}, keep_none=False)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert 'foo' not in ecol['1']
    assert ecol['1']['bar'] is None
    old_rev = result['_rev']

    # Test update edge to a valid edge
    result = ecol.update_one(edge5)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['_from'] == 'vcol1/1'
    assert ecol['1']['_to'] == 'vcol3/5'
    old_rev = result['_rev']

    # Test update edge to a missing edge
    result = ecol.update_one(edge7)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['_from'] == 'vcol1/8'
    assert ecol['1']['_to'] == 'vcol3/7'
    old_rev = result['_rev']

    # TODO why is this succeeding?
    # Test update edge to a invalid edge (from and to mixed up)
    result = ecol.update_one(edge6)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['_from'] == 'vcol3/6'
    assert ecol['1']['_to'] == 'vcol1/2'
    assert ecol['1']['_rev'] != old_rev


@pytest.mark.order17
def test_replace_edge():
    ecol = graph.edge_collection('ecol2')
    ecol.truncate()
    ecol.insert_one(edge1)

    edge = edge1.copy()

    # Test replace edge with a single field change
    assert 'foo' not in ecol.fetch_by_key('1')
    edge['foo'] = 100
    result = ecol.replace_one(edge)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert ecol['1']['foo'] == 100
    old_rev = ecol['1']['_rev']

    # Test replace edge with multiple field changes
    edge['foo'] = 200
    edge['bar'] = 300
    result = ecol.replace_one(edge)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] == 300
    old_rev = result['_rev']

    # Test replace edge with correct revision
    edge['foo'] = 300
    edge['bar'] = 400
    edge['_rev'] = old_rev
    result = ecol.replace_one(edge)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 300
    assert ecol['1']['bar'] == 400
    old_rev = result['_rev']

    # Test replace edge with incorrect revision
    edge['bar'] = 500
    edge['_rev'] = str(int(old_rev) + 10)
    with pytest.raises(DocumentRevisionError):
        ecol.replace_one(edge)
    assert ecol['1']['foo'] == 300
    assert ecol['1']['bar'] == 400

    # Test replace edge with sync option
    edge.update()
    result = ecol.replace_one({'_key': '1', 'bar': 500}, sync=True)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] == 500
    old_rev = result['_rev']

    # Test replace edge without keep_none option
    result = ecol.replace_one({'_key': '1', 'bar': None}, keep_none=True)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['foo'] == 200
    assert ecol['1']['bar'] is None
    old_rev = result['_rev']

    # Test replace edge with keep_none option
    result = ecol.replace_one({'_key': '1', 'foo': None}, keep_none=False)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert 'foo' not in ecol['1']
    assert ecol['1']['bar'] is None
    old_rev = result['_rev']

    # Test replace edge to a valid edge
    result = ecol.replace_one(edge5)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['_from'] == 'vcol1/1'
    assert ecol['1']['_to'] == 'vcol3/5'
    old_rev = result['_rev']

    # Test replace edge to a missing edge
    result = ecol.replace_one(edge7)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['_from'] == 'vcol1/8'
    assert ecol['1']['_to'] == 'vcol3/7'
    old_rev = result['_rev']

    # TODO why is this succeeding?
    # Test replace edge to a invalid edge (from and to mixed up)
    result = ecol.replace_one(edge6)
    assert result['_id'] == 'ecol2/1'
    assert result['_key'] == '1'
    assert result['_old_rev'] == old_rev
    assert ecol['1']['_from'] == 'vcol3/6'
    assert ecol['1']['_to'] == 'vcol1/2'
    assert ecol['1']['_rev'] != old_rev



@pytest.mark.order18
def test_delete_edge():
    ecol = graph.edge_collection('ecol2')


@pytest.mark.order19
def test_traverse():
    pass
