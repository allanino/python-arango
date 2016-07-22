from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango import ArangoClient
from arango.collection import Collection
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    clean_keys
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
db = arango_client.create_database(db_name)
col_name = generate_col_name(db)
col = db.create_collection(col_name)
col.add_geo_index(['coordinates'])


def teardown_module(*_):
    arango_client.delete_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


def test_properties():
    assert col.name == col_name
    assert repr(col) == '<ArangoDB collection "{}">'.format(col_name)
    properties = col.properties()
    assert 'id' in properties
    assert properties['status'] in Collection.STATUSES.values()
    assert properties['name'] == col_name
    assert properties['edge'] is False
    assert properties['system'] is False
    assert isinstance(properties['sync'], bool)
    assert isinstance(properties['compact'], bool)
    assert isinstance(properties['volatile'], bool)
    assert isinstance(properties['journal_size'], int)
    assert properties['keygen'] in ('autoincrement', 'traditional')
    assert isinstance(properties['user_keys'], bool)
    if 'key_increment' in properties:
        assert isinstance(properties['key_increment'], int)
    if 'key_offset' in properties:
        assert isinstance(properties['key_offset'], int)


def test_set_properties():
    properties = col.properties()
    old_sync = properties['sync']
    old_journal_size = properties['journal_size']

    new_sync = not old_sync
    new_journal_size = old_journal_size + 1
    result = col.set_properties(
        sync=new_sync, journal_size=new_journal_size
    )
    assert result['sync'] == new_sync
    assert result['journal_size'] == new_journal_size

    new_properties = col.properties()
    assert new_properties['sync'] == new_sync
    assert new_properties['journal_size'] == new_journal_size


def test_rename():
    assert col.name == col_name
    new_name = generate_col_name(db)

    result = col.rename(new_name)
    assert result['name'] == new_name
    assert col.name == new_name
    assert repr(col) == '<ArangoDB collection "{}">'.format(new_name)

    # Try again (the operation should be idempotent)
    result = col.rename(new_name)
    assert result['name'] == new_name
    assert col.name == new_name
    assert repr(col) == '<ArangoDB collection "{}">'.format(new_name)


def test_statistics():
    stats = col.statistics()
    assert 'alive' in stats
    assert 'compactors' in stats
    assert 'dead' in stats
    assert 'document_refs' in stats
    assert 'journals' in stats


def test_revision():
    revision = col.revision()
    assert isinstance(revision, string_types)


def test_load():
    status = col.load()
    assert status in {'loaded', 'loading'}


def test_unload():
    status = col.unload()
    assert status in {'unloaded', 'unloading'}


def test_rotate():
    # No journal should exist yet
    with pytest.raises(CollectionRotateError):
        col.rotate()


def test_checksum():
    assert col.checksum(include_rev=True, include_data=False) == 0
    assert col.checksum(include_rev=True, include_data=True) == 0
    assert col.checksum(include_rev=False, include_data=False) == 0
    assert col.checksum(include_rev=False, include_data=True) == 0

    col.insert_one({'value': 'bar'})
    assert col.checksum(include_rev=True, include_data=False) > 0
    assert col.checksum(include_rev=True, include_data=True) > 0
    assert col.checksum(include_rev=False, include_data=False) > 0
    assert col.checksum(include_rev=False, include_data=True) > 0


def test_truncate():
    col.insert_one({'value': 'bar'})
    col.insert_one({'value': 'bar'})
    assert len(col) > 1

    result = col.truncate()
    assert 'id' in result
    assert 'name' in result
    assert 'status' in result
    assert 'is_system' in result
    assert len(col) == 0


def test_insert_one():
    for i in range(1, 6):
        doc = col.insert_one({'_key': str(i), 'value': i * 100})
        assert doc['_key'] == str(i)
        assert doc['_id'] == '{}/{}'.format(col.name, str(i))
        assert doc['_rev'].isdigit()

    assert len(col) == 5
    for key in range(1, 6):
        assert key in col
        document = col.fetch_by_key(key)
        assert document['_key'] == str(key)
        assert document['value'] == key * 100

    assert '6' not in col
    col.insert_one({'_key': '6', 'value': 200}, sync=True)
    assert '6' in col
    assert col.fetch_by_key('6')['value'] == 200

    with pytest.raises(DocumentInsertError):
        col.insert_one({'_key': '1', 'value': 300})
    assert col['1']['value'] == 100


def test_insert_many():
    result = col.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
        {'_key': '4', 'value': 400},
        {'_key': '5', 'value': 500},
    ])
    assert result['created'] == 5
    assert result['errors'] == 0
    assert 'details' in result
    assert len(col) == 5
    for key in range(1, 6):
        assert key in col
        document = col.fetch_by_key(key)
        assert document['_key'] == str(key)
        assert document['value'] == key * 100

    with pytest.raises(DocumentInsertError):
        col.insert_many([
            {'_key': '1', 'value': 100},
            {'_key': '1', 'value': 200},
            {'_key': '1', 'value': 300},
        ], halt_on_error=True)

    result = col.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '1', 'value': 200},
        {'_key': '1', 'value': 300},
    ], halt_on_error=False, details=True)
    assert result['created'] == 0
    assert result['errors'] == 3
    assert 'details' in result

    result = col.insert_many([
        {'_key': '6', 'value': 100},
        {'_key': '7', 'value': 200},
        {'_key': '8', 'value': 300},
    ], details=False)
    assert 'details' not in result


def test_update():
    assert col.update({'value': 100}, {'bar': 100}) == 0
    col.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 100},
        {'_key': '3', 'value': 100},
        {'_key': '4', 'value': 200},
        {'_key': '5', 'value': 300},
    ])

    assert col.update({'value': 200}, {'bar': 100}) == 1
    assert col['4']['value'] == 200
    assert col['4']['bar'] == 100

    assert col.update({'value': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert col[key]['value'] == 100
        assert col[key]['bar'] == 100

    assert col['5']['value'] == 300
    assert 'bar' not in col['5']

    assert col.update(
        {'value': 300}, {'value': None}, sync=True, keep_none=True
    ) == 1
    assert col['5']['value'] is None
    assert col.update(
        {'value': 200}, {'value': None}, sync=True, keep_none=False
    ) == 1
    assert 'value' not in col['4']


def test_update_one():
    doc = {'_key': '1', 'value': 100}
    col.insert_one(doc)

    doc['value'] = 200
    doc = col.update_one(doc)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert col['1']['value'] == 200
    old_rev = doc['_rev']

    doc['value'] = None
    doc = col.update_one(doc, keep_none=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert doc['_old_rev'] == old_rev
    assert col['1']['value'] is None
    old_rev = doc['_rev']

    doc['value'] = {'bar': 1}
    doc = col.update_one(doc, sync=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert doc['_old_rev'] == old_rev
    assert col['1']['value'] == {'bar': 1}
    old_rev = doc['_rev']

    doc['value'] = {'baz': 2}
    doc = col.update_one(doc, merge=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert doc['_old_rev'] == old_rev
    assert col['1']['value'] == {'bar': 1, 'baz': 2}
    old_rev = doc['_rev']

    doc['value'] = None
    doc = col.update_one(doc, keep_none=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert doc['_old_rev'] == old_rev
    assert 'value' not in col['1']

    doc['value'] = 300
    doc['_rev'] = str(int(doc['_rev']) + 1)
    with pytest.raises(DocumentRevisionError):
        col.update_one(doc)
    assert 'value' not in col['1']

    with pytest.raises(DocumentUpdateError):
        col.update_one({'_key': '2', 'value': 300})
    assert 'value' not in col['1']


def test_replace():
    assert col.replace({'value': 100}, {'bar': 100}) == 0
    col.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 100},
        {'_key': '3', 'value': 100},
        {'_key': '4', 'value': 200},
        {'_key': '5', 'value': 300},
    ])

    assert col.replace({'value': 200}, {'bar': 100}) == 1
    assert 'value' not in col['4']
    assert col['4']['bar'] == 100

    assert col.replace({'value': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert 'value' not in col[key]
        assert col[key]['bar'] == 100

    assert col['5']['value'] == 300
    assert 'bar' not in col['5']


def test_replace_one():
    doc = {'_key': '1', 'value': 100}
    col.insert_one(doc)

    doc['value'] = 200
    doc = col.replace_one(doc)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert col['1']['value'] == 200
    old_rev = doc['_rev']

    doc['value'] = 300
    doc = col.replace_one(doc, sync=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert doc['_old_rev'] == old_rev
    assert col['1']['value'] == 300
    old_rev = doc['_rev']

    doc['value'] = 400
    del doc['_rev']
    doc = col.replace_one(doc)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert doc['_rev'].isdigit()
    assert doc['_old_rev'] == old_rev
    assert col['1']['value'] == 400

    doc['value'] = 500
    doc['_rev'] = str(int(doc['_rev']) + 1)
    with pytest.raises(DocumentRevisionError):
        col.replace_one(doc)
    assert col['1']['value'] == 400

    with pytest.raises(DocumentReplaceError):
        col.replace_one({'_key': '2', 'value': 600})
    assert col['1']['value'] == 400


def test_delete():
    assert col.delete({'value': 100}) == 0
    col.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 100},
        {'_key': '3', 'value': 100},
        {'_key': '4', 'value': 200},
        {'_key': '5', 'value': 300},
    ])

    assert '4' in col
    assert col.delete({'value': 200}) == 1
    assert '4' not in col

    assert '5' in col
    assert col.delete({'value': 300}, sync=True) == 1
    assert '5' not in col

    assert col.delete({'value': 100}, limit=2) == 2
    count = 0
    for key in ['1', '2', '3']:
        if key in col:
            assert col[key]['value'] == 100
            count += 1
    assert count == 1


def test_delete_one():
    doc1 = {'_key': '1', 'value': 100}
    doc2 = {'_key': '2', 'value': 200}
    doc3 = {'_key': '3', 'value': 300}
    doc4 = {'_key': '4', 'value': 300}
    col.insert_many([doc1, doc2, doc3])

    result = col.delete_one(doc1)
    assert result['_id'] == '{}/1'.format(col.name)
    assert result['_key'] == '1'
    assert result['_rev'].isdigit()
    assert '_old_rev' not in result
    assert '1' not in col
    assert len(col) == 2

    result = col.delete_one(doc2, sync=True)
    assert result['_id'] == '{}/2'.format(col.name)
    assert result['_key'] == '2'
    assert result['_rev'].isdigit()
    assert '_old_rev' not in result
    assert '2' not in col
    assert len(col) == 1

    doc3['_rev'] = str(int(col['3']['_rev']) + 1)
    with pytest.raises(DocumentRevisionError):
        col.delete_one(doc3)
    assert '3' in col
    assert len(col) == 1

    assert col.delete_one(doc4, ignore_missing=True) is False
    with pytest.raises(DocumentDeleteError):
        col.delete_one(doc4, ignore_missing=False)
    assert len(col) == 1


def test_delete_by_keys():
    result = col.delete_by_keys(['1', '2', '3'])
    assert result['removed'] == 0
    assert result['ignored'] == 3

    col.insert_many([
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
    ])
    result = col.delete_by_keys([])
    assert result['removed'] == 0
    assert result['ignored'] == 0
    for key in ['1', '2', '3']:
        assert key in col

    result = col.delete_by_keys(['1'])
    assert result['removed'] == 1
    assert result['ignored'] == 0
    assert '1' not in col
    assert len(col) == 2

    result = col.delete_by_keys(['4'])
    assert result['removed'] == 0
    assert result['ignored'] == 1
    assert '2' in col and '3' in col
    assert len(col) == 2

    result = col.delete_by_keys(['1', '2', '3'])
    assert result['removed'] == 2
    assert result['ignored'] == 1
    assert len(col) == 0


def test_fetch():
    assert list(col.fetch({'value': 100})) == []
    inserted = [
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 100},
        {'_key': '3', 'value': 100},
        {'_key': '4', 'value': 200},
        {'_key': '5', 'value': 300},
    ]
    col.insert_many(inserted)

    found = list(col.fetch({'value': 100}))
    assert len(found) == 3
    for doc in map(dict, found):
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'value': doc['value']} in inserted

    found = list(col.fetch({'value': 100}, offset=1))
    assert len(found) == 2
    for doc in map(dict, found):
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'value': doc['value']} in inserted

    found = list(col.fetch({}, limit=4))
    assert len(found) == 4
    for doc in map(dict, found):
        assert doc['_key'] in ['1', '2', '3', '4', '5']
        assert {'_key': doc['_key'], 'value': doc['value']} in inserted

    found = list(col.fetch({'value': 200}))
    assert len(found) == 1
    assert found[0]['_key'] == '4'


def test_fetch_by_key():
    col.insert_one({'_key': '1', 'value': 100})
    doc = col.fetch_by_key('1')
    assert doc['value'] == 100

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    assert col.fetch_by_key('2') is None
    assert col.fetch_by_key('1', rev=old_rev) == doc

    with pytest.raises(DocumentRevisionError):
        col.fetch_by_key('1', rev=new_rev)


def test_fetch_by_keys():
    assert col.fetch_by_keys(['1', '2', '3', '4', '5']) == []
    expected = [
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
        {'_key': '4', 'value': 400},
        {'_key': '5', 'value': 500},
    ]
    col.insert_many(expected)
    assert col.fetch_by_keys([]) == []
    assert expected == [
        {'_key': doc['_key'], 'value': doc['value']}
        for doc in col.fetch_by_keys(['1', '2', '3', '4', '5'])
    ]
    assert expected == [
        {'_key': doc['_key'], 'value': doc['value']}
        for doc in col.fetch_by_keys(['1', '2', '3', '4', '5', '6'])
    ]


def test_fetch_all():
    assert len(list(col.fetch_all())) == 0
    inserted = [
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
        {'_key': '4', 'value': 400},
        {'_key': '5', 'value': 500},
    ]
    for doc in inserted:
        col.insert_one(doc)
    fetched = list(col.fetch_all())
    assert len(fetched) == len(inserted)
    for doc in map(dict, fetched):
        assert {'_key': doc['_key'], 'value': doc['value']} in inserted

    # TODO ordering is strange
    assert len(list(col.fetch_all(offset=5))) == 0
    fetched = list(col.fetch_all(offset=3))
    assert len(fetched) == 2

    # TODO ordering is strange
    assert len(list(col.fetch_all(limit=0))) == 0
    fetched = list(col.fetch_all(limit=2))
    assert len(fetched) == 2


def test_fetch_random():
    assert len(list(col.fetch_all())) == 0
    inserted = [
        {'_key': '1', 'value': 100},
        {'_key': '2', 'value': 200},
        {'_key': '3', 'value': 300},
        {'_key': '4', 'value': 400},
        {'_key': '5', 'value': 500},
    ]
    col.insert_many(inserted)
    for attempt in range(10):
        doc = col.fetch_random()
        assert {'_key': doc['_key'], 'value': doc['value']} in inserted


def test_fetch_near():
    col.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
        {'_key': '2', 'coordinates': [2, 2]},
        {'_key': '3', 'coordinates': [3, 3]},
    ])
    result = col.fetch_near(
        latitude=1,
        longitude=1,
        limit=2
    )
    expected = [
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [2, 2]}
    ]
    assert clean_keys(list(result)) == expected

    result = col.fetch_near(
        latitude=4,
        longitude=4,
    )
    expected = [
        {'_key': '4', 'coordinates': [4, 4]},
        {'_key': '3', 'coordinates': [3, 3]},
        {'_key': '2', 'coordinates': [2, 2]},
        {'_key': '1', 'coordinates': [1, 1]},
    ]
    assert clean_keys(list(result)) == expected


def test_fetch_in_range():
    col.add_skiplist_index(['value'])
    col.insert_many([
        {'_key': '1', 'value': 1},
        {'_key': '2', 'value': 2},
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
        {'_key': '5', 'value': 5}
    ])
    result = col.fetch_in_range(
        field='value',
        lower=2,
        upper=5,
        offset=1,
        limit=2,
    )
    expected = [
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
    ]
    assert clean_keys(list(result)) == expected


# TODO the WITHIN geo function does not seem to work properly
def test_fetch_in_radius():
    col.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 4]},
        {'_key': '3', 'coordinates': [4, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
    ])
    result = list(col.fetch_in_radius(3, 3, 10, 'distance'))
    for doc in result:
        assert 'distance' in doc


def test_fetch_in_box():
    col.insert_many([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 5]},
        {'_key': '3', 'coordinates': [5, 1]},
        {'_key': '4', 'coordinates': [5, 5]},
    ])
    result = col.fetch_in_box(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3
    )
    expected = [
        {'_key': '3', 'coordinates': [5, 1]},
        {'_key': '1', 'coordinates': [1, 1]}
    ]
    assert clean_keys(list(result)) == expected

    result = col.fetch_in_box(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3,
        limit=1
    )
    expected = [
        {'_key': '3', 'coordinates': [5, 1]}
    ]
    assert clean_keys(list(result)) == expected

    result = col.fetch_in_box(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3,
        skip=1
    )
    expected = [
        {'_key': '1', 'coordinates': [1, 1]}
    ]
    assert clean_keys(list(result)) == expected


def test_fetch_by_text():
    col.add_fulltext_index(['text'])
    col.insert_many([
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'},
        {'_key': '3', 'text': 'baz'}
    ])
    result = col.fetch_by_text(
        key='text', query='foo,|bar'
    )
    expected = [
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'}
    ]
    assert clean_keys(list(result)) == expected

    # Bad parameter
    with pytest.raises(DocumentFetchError):
        col.fetch_by_text(key='text', query='+')

    with pytest.raises(DocumentFetchError):
        col.fetch_by_text(key='text', query='|')


def test_list_indexes():
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'primary',
        'fields': ['_key'],
        'unique': True
    }
    indexes = col.indexes()
    assert isinstance(indexes, dict)
    assert expected_index in indexes.values()


def test_add_hash_index():
    col.add_hash_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'hash',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in col.indexes().values()


def test_add_skiplist_index():
    col.add_skiplist_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'sparse': False,
        'type': 'skiplist',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in col.indexes().values()


def test_add_geo_index():
    # With one attribute
    col.add_geo_index(
        fields=['attr1'],
        ordered=False,
    )
    expected_index = {
        'sparse': True,
        'type': 'geo1',
        'fields': ['attr1'],
        'unique': False,
        'geo_json': False,
        'ignore_none': True,
        'constraint': False
    }
    assert expected_index in col.indexes().values()

    # With two attributes
    col.add_geo_index(
        fields=['attr1', 'attr2'],
        ordered=False,
    )
    expected_index = {
        'sparse': True,
        'type': 'geo2',
        'fields': ['attr1', 'attr2'],
        'unique': False,
        'ignore_none': True,
        'constraint': False
    }
    assert expected_index in col.indexes().values()

    # With more than two attributes (should fail)
    with pytest.raises(IndexCreateError):
        col.add_geo_index(fields=['attr1', 'attr2', 'attr3'])


def test_add_fulltext_index():
    # With two attributes (should fail)
    with pytest.raises(IndexCreateError):
        col.add_fulltext_index(fields=['attr1', 'attr2'])

    col.add_fulltext_index(
        fields=['attr1'],
        minimum_length=10,
    )
    expected_index = {
        'sparse': True,
        'type': 'fulltext',
        'fields': ['attr1'],
        'min_length': 10,
        'unique': False,
    }
    assert expected_index in col.indexes().values()


def test_delete_index():
    old_indexes = set(col.indexes())
    col.add_hash_index(['attr1', 'attr2'], unique=True)
    col.add_skiplist_index(['attr1', 'attr2'], unique=True)
    col.add_fulltext_index(fields=['attr1'], minimum_length=10)

    new_indexes = set(col.indexes())
    assert new_indexes.issuperset(old_indexes)

    for index_id in new_indexes - old_indexes:
        col.delete_index(index_id)
    assert set(col.indexes()) == old_indexes
