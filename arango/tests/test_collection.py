from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango.connection import Connection
from arango.constants import COLLECTION_STATUSES
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    clean_keys
)

conn = Connection()
db_name = generate_db_name(conn)
db = conn.create_database(db_name)
col_name = generate_col_name(db)
col = db.create_collection(col_name)
col.add_geo_index(['coordinates'])


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    col.truncate()


def test_properties():
    assert col.name == col_name
    assert repr(col) == (
        "<ArangoDB document collection '{}'>".format(col_name)
    )


def test_options():
    options = col.properties()
    assert 'id' in options
    assert options['status'] in COLLECTION_STATUSES.values()
    assert options['name'] == col_name
    assert options['edge'] == False
    assert options['system'] == False
    assert isinstance(options['sync'], bool)
    assert isinstance(options['compact'], bool)
    assert isinstance(options['volatile'], bool)
    assert isinstance(options['journal_size'], int)
    assert options['keygen'] in ('autoincrement', 'traditional')
    assert isinstance(options['user_keys'], bool)
    if 'key_increment' in options:
        assert isinstance(options['key_increment'], int)
    if 'key_offset' in options:
        assert isinstance(options['key_offset'], int)


def test_set_options():
    options = col.properties()
    old_sync = options['sync']
    old_journal_size = options['journal_size']

    new_sync = not old_sync
    new_journal_size = old_journal_size + 1
    result = col.set_properties(
        sync=new_sync, journal_size=new_journal_size
    )
    assert isinstance(result, bool)
    new_options = col.properties()
    assert new_options['sync'] == new_sync
    assert new_options['journal_size'] == new_journal_size


def test_rename():
    assert col.name == col_name
    new_name = generate_col_name(db)

    result = col.rename(new_name)
    assert result is True
    assert col.name == new_name
    assert repr(col) == (
        "<ArangoDB document collection '{}'>".format(new_name)
    )

    # Try again (the operation should be idempotent)
    result = col.rename(new_name)
    assert result is True
    assert col.name == new_name
    assert repr(col) == (
        "<ArangoDB document collection '{}'>".format(new_name)
    )


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
    assert status in ('loaded', 'loading')


def test_unload():
    status = col.unload()
    assert status in ('unloaded', 'unloading')


def test_rotate():
    # No journal should exist yet
    with pytest.raises(CollectionRotateError):
        col.rotate()


def test_checksum():
    assert col.checksum(revision=True, data=False) == 0
    assert col.checksum(revision=True, data=True) == 0
    assert col.checksum(revision=False, data=False) == 0
    assert col.checksum(revision=False, data=True) == 0

    col.insert({'foo': 'bar'})
    assert col.checksum(revision=True, data=False) > 0
    assert col.checksum(revision=True, data=True) > 0
    assert col.checksum(revision=False, data=False) > 0
    assert col.checksum(revision=False, data=True) > 0


def test_truncate():
    col.insert({'foo': 'bar'})
    col.insert({'foo': 'bar'})
    assert len(col) > 1

    result = col.truncate()
    assert isinstance(result, bool)
    assert len(col) == 0


def test_insert():
    for i in range(1, 6):
        doc = col.insert({'_key': str(i), 'foo': i * 100})
        assert doc['_id'] == '{}/{}'.format(col.name, str(i))
        assert doc['_key'] == str(i)

    assert len(col) == 5
    for key in range(1, 6):
        assert key in col
        document = col.get(key)
        assert document['_key'] == str(key)
        assert document['foo'] == key * 100

    assert '6' not in col
    col.insert({'_key': '6', 'foo': 200}, sync=True)
    assert '6' in col
    assert col.get('6')['foo'] == 200

    with pytest.raises(DocumentInsertError):
        col.insert({'_key': '1', 'foo': 300})
    assert col['1']['foo'] == 100


def test_insert_many():
    result = col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ])
    assert result['created'] == 5
    assert result['errors'] == 0
    assert 'details' in result
    assert len(col) == 5
    for key in range(1, 6):
        assert key in col
        document = col.get(key)
        assert document['_key'] == str(key)
        assert document['foo'] == key * 100

    with pytest.raises(DocumentInsertError):
        col.insert_bulk([
            {'_key': '1', 'foo': 100},
            {'_key': '1', 'foo': 200},
            {'_key': '1', 'foo': 300},
        ], halt_on_error=True)

    result = col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '1', 'foo': 200},
        {'_key': '1', 'foo': 300},
    ], halt_on_error=False, details=True)
    assert result['created'] == 0
    assert result['errors'] == 3
    assert 'details' in result

    result = col.insert_bulk([
        {'_key': '6', 'foo': 100},
        {'_key': '7', 'foo': 200},
        {'_key': '8', 'foo': 300},
    ], details=False)
    assert 'details' not in result


def test_get():
    col.insert({'_key': '1', 'foo': 100})
    doc = col.get('1')
    assert doc['foo'] == 100

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    assert col.get('2') is None
    assert col.get('1', rev=old_rev) == doc

    with pytest.raises(DocumentRevisionError):
        col.get('1', rev=new_rev)


def test_get_many():
    assert col.gets(['1', '2', '3', '4', '5']) == []
    expected = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    col.insert_bulk(expected)
    assert col.gets([]) == []
    assert expected == [
        {'_key': doc['_key'], 'foo': doc['foo']}
        for doc in col.gets(['1', '2', '3', '4', '5'])
        ]
    assert expected == [
        {'_key': doc['_key'], 'foo': doc['foo']}
        for doc in col.gets(['1', '2', '3', '4', '5', '6'])
        ]


def test_update():
    col.insert({'_key': '1', 'foo': 100})
    assert col['1']['foo'] == 100

    doc = col.update_by_key('1', {'foo': 200})
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == 200

    doc = col.update_by_key('1', {'foo': None}, keep_none=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] is None
    
    doc = col.update_by_key('1', {'foo': {'bar': 1}}, sync=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == {'bar': 1}

    doc = col.update_by_key('1', {'foo': {'baz': 2}}, merge=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == {'bar': 1, 'baz': 2}

    doc = col.update_by_key('1', {'foo': None}, keep_none=False)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert 'foo' not in col['1']

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        col.update_by_key('1', {'foo': 300, '_rev': new_rev})
    assert 'foo' not in col['1']

    with pytest.raises(DocumentUpdateError):
        col.update_by_key('2', {'foo': 300})
    assert 'foo' not in col['1']


def test_replace():
    doc = col.insert({'_key': '1', 'foo': 100})
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == 100

    doc = col.replace_by_key('1', {'foo': 200})
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == 200

    doc = col.replace_by_key('1', {'foo': 300}, sync=True)
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == 300

    doc = col.replace_by_key('1', {'foo': 400}, revision=doc['_rev'])
    assert doc['_id'] == '{}/1'.format(col.name)
    assert doc['_key'] == '1'
    assert col['1']['foo'] == 400

    old_rev = doc['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        col.replace_by_key('1', {'foo': 500, '_rev': new_rev})
    assert col['1']['foo'] == 400

    with pytest.raises(DocumentReplaceError):
        col.replace_by_key('2', {'foo': 600})
    assert col['1']['foo'] == 400


def test_delete():
    col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ])

    doc = col.delete('1')
    assert doc['id'] == '{}/1'.format(col.name)
    assert doc['key'] == '1'
    assert '1' not in col
    assert len(col) == 2

    doc = col.delete('2', sync=True)
    assert doc['id'] == '{}/2'.format(col.name)
    assert doc['key'] == '2'
    assert '2' not in col
    assert len(col) == 1

    old_rev = col['3']['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        col.delete('3', rev=new_rev)
    assert '3' in col
    assert len(col) == 1

    assert col.delete('4') == False
    with pytest.raises(DocumentDeleteError):
        col.delete('4', ignore_missing=False)
    assert len(col) == 1


def test_delete_many():
    result = col.deletes(['1', '2', '3'])
    assert result['removed'] == 0
    assert result['ignored'] == 3

    col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
    ])
    result = col.deletes([])
    assert result['removed'] == 0
    assert result['ignored'] == 0
    for key in ['1', '2', '3']:
        assert key in col

    result = col.deletes(['1'])
    assert result['removed'] == 1
    assert result['ignored'] == 0
    assert '1' not in col
    assert len(col) == 2

    result = col.deletes(['4'])
    assert result['removed'] == 0
    assert result['ignored'] == 1
    assert '2' in col and '3' in col
    assert len(col) == 2

    result = col.deletes(['1', '2', '3'])
    assert result['removed'] == 2
    assert result['ignored'] == 1
    assert len(col) == 0


def test_all():
    assert len(list(col.all())) == 0
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    for doc in inserted:
        col.insert(doc)
    fetched = list(col.all())
    assert len(fetched) == len(inserted)
    for doc in fetched:
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    # TODO ordering is strange
    assert len(list(col.all(offset=5))) == 0
    fetched = list(col.all(offset=3))
    assert len(fetched) == 2

    # TODO ordering is strange
    assert len(list(col.all(limit=0))) == 0
    fetched = list(col.all(limit=2))
    assert len(fetched) == 2


def test_random():
    assert len(list(col.all())) == 0
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 200},
        {'_key': '3', 'foo': 300},
        {'_key': '4', 'foo': 400},
        {'_key': '5', 'foo': 500},
    ]
    col.insert_bulk(inserted)
    for attempt in range(10):
        doc = col.random()
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted


def test_find_one():
    assert col.find_one({'foo': 100}) is None
    assert col.find_one({}) is None
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ]
    col.insert_bulk(inserted)

    assert col.find_one({'_key': '6'}) is None
    assert col.find_one({'foo': 400}) is None
    assert col.find_one({'baz': 100}) is None
    assert col.find_one({}) is not None

    for i in [100, 200, 300]:
        assert col.find_one({'foo': i})['foo'] == i
    for i in range(1, 6):
        assert col.find_one({'_key': str(i)})['_key'] == str(i)


def test_find_many():
    assert list(col.find({'foo': 100})) == []
    inserted = [
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ]
    col.insert_bulk(inserted)

    found = list(col.find({'foo': 100}))
    assert len(found) == 3
    for doc in found:
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(col.find({'foo': 100}, offset=1))
    assert len(found) == 2
    for doc in found:
        assert doc['_key'] in ['1', '2', '3']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(col.find({}, limit=4))
    assert len(found) == 4
    for doc in found:
        assert doc['_key'] in ['1', '2', '3', '4', '5']
        assert {'_key': doc['_key'], 'foo': doc['foo']} in inserted

    found = list(col.find({'foo': 200}))
    assert len(found) == 1
    assert found[0]['_key'] == '4'


def test_find_and_update():
    assert col.update({'foo': 100}, {'bar': 100}) == 0
    col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert col.update({'foo': 200}, {'bar': 100}) == 1
    assert col['4']['foo'] == 200
    assert col['4']['bar'] == 100

    assert col.update({'foo': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert col[key]['foo'] == 100
        assert col[key]['bar'] == 100

    assert col['5']['foo'] == 300
    assert 'bar' not in col['5']

    assert col.update(
        {'foo': 300}, {'foo': None}, sync=True, keep_none=True
    ) == 1
    assert col['5']['foo'] is None
    assert col.update(
        {'foo': 200}, {'foo': None}, sync=True, keep_none=False
    ) == 1
    assert 'foo' not in col['4']


def test_find_and_replace():
    assert col.replace({'foo': 100}, {'bar': 100}) == 0
    col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert col.replace({'foo': 200}, {'bar': 100}) == 1
    assert 'foo' not in col['4']
    assert col['4']['bar'] == 100

    assert col.replace({'foo': 100}, {'bar': 100}) == 3
    for key in ['1', '2', '3']:
        assert 'foo' not in col[key]
        assert col[key]['bar'] == 100

    assert col['5']['foo'] == 300
    assert 'bar' not in col['5']


def test_find_and_delete():
    assert col.delete({'foo': 100}) == 0
    col.insert_bulk([
        {'_key': '1', 'foo': 100},
        {'_key': '2', 'foo': 100},
        {'_key': '3', 'foo': 100},
        {'_key': '4', 'foo': 200},
        {'_key': '5', 'foo': 300},
    ])

    assert '4' in col
    assert col.delete({'foo': 200}) == 1
    assert '4' not in col

    assert '5' in col
    assert col.delete({'foo': 300}, sync=True) == 1
    assert '5' not in col

    assert col.delete({'foo': 100}, limit=2) == 2
    count = 0
    for key in ['1', '2', '3']:
        if key in col:
            assert col[key]['foo'] == 100
            count += 1
    assert count == 1


def test_find_near():
    col.insert_bulk([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
        {'_key': '2', 'coordinates': [2, 2]},
        {'_key': '3', 'coordinates': [3, 3]},
    ])
    result = col.find_near(
        latitude=1,
        longitude=1,
        limit=2
    )
    expected = [
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [2, 2]}
    ]
    assert clean_keys(list(result)) == expected

    result = col.find_near(
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


def test_find_in_range():
    col.add_skiplist_index(['value'])
    col.insert_bulk([
        {'_key': '1', 'value': 1},
        {'_key': '2', 'value': 2},
        {'_key': '3', 'value': 3},
        {'_key': '4', 'value': 4},
        {'_key': '5', 'value': 5}
    ])
    result = col.find_in_range(
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
def test_find_in_radius():
    col.insert_bulk([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 4]},
        {'_key': '3', 'coordinates': [4, 1]},
        {'_key': '4', 'coordinates': [4, 4]},
    ])
    result = list(col.find_in_radius(3, 3, 10, 'distance'))
    for doc in result:
        assert 'distance' in doc


def test_find_in_rectangle():
    col.insert_bulk([
        {'_key': '1', 'coordinates': [1, 1]},
        {'_key': '2', 'coordinates': [1, 5]},
        {'_key': '3', 'coordinates': [5, 1]},
        {'_key': '4', 'coordinates': [5, 5]},
    ])
    result = col.find_in_box(
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

    result = col.find_in_box(
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

    result = col.find_in_box(
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


def test_find_text():
    col.add_fulltext_index(['text'])
    col.insert_bulk([
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'},
        {'_key': '3', 'text': 'baz'}
    ])
    result = col.find_text(
        field='text', query='foo,|bar'
    )
    expected = [
        {'_key': '1', 'text': 'foo'},
        {'_key': '2', 'text': 'bar'}
    ]
    assert clean_keys(list(result)) == expected

    # Bad parameter
    with pytest.raises(DocumentFindTextError):
        col.find_text(field='text', query='+')

    with pytest.raises(DocumentFindTextError):
        col.find_text(field='text', query='|')


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
        geo_json=False,
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
        geo_json=False,
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
        min_length=10,
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
    col.add_fulltext_index(fields=['attr1'], min_length=10)

    new_indexes = set(col.indexes())
    assert new_indexes.issuperset(old_indexes)

    for index_id in new_indexes - old_indexes:
        col.delete_index(index_id)
    assert set(col.indexes()) == old_indexes
