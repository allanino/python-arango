from __future__ import absolute_import, unicode_literals

import pytest
from six import string_types

from arango import ArangoClient
from arango.constants import COLLECTION_STATUSES
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name,
    clean_keys
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
db = arango_client.create_database(db_name)
ecol_name = generate_col_name(db)
ecol = db.create_collection(ecol_name, edge=True)
ecol.add_geo_index(['coordinates'])

# Set up test collection and edges
col = generate_col_name(db)
db.create_collection(col).insert_many([
    {'_key': '1'}, {'_key': '2'}, {'_key': '3'}, {'_key': '4'}
])
edge1 = {'_key': '1', '_from': '{}/1'.format(col), '_to': '{}/2'.format(col)}
edge2 = {'_key': '2', '_from': '{}/2'.format(col), '_to': '{}/3'.format(col)}
edge3 = {'_key': '3', '_from': '{}/3'.format(col), '_to': '{}/4'.format(col)}
edge4 = {'_key': '4', '_from': '{}/4'.format(col), '_to': '{}/1'.format(col)}


def teardown_module(*_):
    arango_client.drop_database(db_name, ignore_missing=True)


def setup_function(*_):
    ecol.truncate()


def test_properties():
    assert ecol.name == ecol_name
    assert repr(ecol) == ("<ArangoDB collection '{}'>".format(ecol_name))


def test_options():
    options = ecol.properties()
    assert 'id' in options
    assert options['status'] in COLLECTION_STATUSES.values()
    assert options['name'] == ecol_name
    assert options['edge'] == True
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
    options = ecol.properties()
    old_sync = options['sync']
    old_journal_size = options['journal_size']

    new_sync = not old_sync
    new_journal_size = old_journal_size + 1
    result = ecol.set_properties(
        sync=new_sync, journal_size=new_journal_size
    )
    assert isinstance(result, bool)
    new_options = ecol.properties()
    assert new_options['sync'] == new_sync
    assert new_options['journal_size'] == new_journal_size


def test_rename():
    assert ecol.name == ecol_name
    new_name = generate_col_name(db)

    result = ecol.rename(new_name)
    assert result is True
    assert ecol.name == new_name
    assert repr(ecol) == "<ArangoDB collection '{}'>".format(new_name)

    # Try again (the operation should be idempotent)
    result = ecol.rename(new_name)
    assert result is True
    assert ecol.name == new_name
    assert repr(ecol) == "<ArangoDB collection '{}'>".format(new_name)


def test_statistics():
    stats = ecol.statistics()
    assert 'alive' in stats
    assert 'compactors' in stats
    assert 'dead' in stats
    assert 'document_refs' in stats
    assert 'journals' in stats


def test_revision():
    revision = ecol.revision()
    assert isinstance(revision, string_types)


def test_load():
    status = ecol.load()
    assert status in ('loaded', 'loading')


def test_unload():
    status = ecol.unload()
    assert status in ('unloaded', 'unloading')


def test_rotate():
    # No journal should exist yet
    with pytest.raises(CollectionRotateError):
        ecol.rotate()


def test_checksum():
    assert ecol.checksum(revision=True, data=False) == 0
    assert ecol.checksum(revision=True, data=True) == 0
    assert ecol.checksum(revision=False, data=False) == 0
    assert ecol.checksum(revision=False, data=True) == 0

    ecol.insert(edge1)
    assert ecol.checksum(revision=True, data=False) > 0
    assert ecol.checksum(revision=True, data=True) > 0
    assert ecol.checksum(revision=False, data=False) > 0
    assert ecol.checksum(revision=False, data=True) > 0


def test_truncate():
    ecol.insert(edge1)
    ecol.insert(edge2)
    assert len(ecol) > 1

    result = ecol.truncate()
    assert isinstance(result, bool)
    assert len(ecol) == 0


def test_insert():
    assert '1' not in ecol
    edge = ecol.insert(edge1)
    assert edge['_key'] == '1'
    assert '1' in ecol
    assert len(ecol) == 1

    edge = ecol.get('1')
    assert edge['_key'] == '1'
    assert edge['_from'] == edge1['_from']
    assert edge['_to'] == edge1['_to']

    assert '2' not in ecol
    edge = ecol.insert(edge2, sync=True)
    assert edge['_key'] == '2'
    assert '2' in ecol
    assert len(ecol) == 2

    edge = ecol.get('2')
    assert edge['_key'] == '2'
    assert edge['_from'] == edge2['_from']
    assert edge['_to'] == edge2['_to']

    with pytest.raises(DocumentInsertError):
        ecol.insert(edge1)

    with pytest.raises(DocumentInsertError):
        ecol.insert(edge2)


def test_insert_many():
    result = ecol.insert_many([edge1, edge2, edge3])
    assert result['created'] == 3
    assert result['errors'] == 0
    assert 'details' in result
    assert len(ecol) == 3
    for key in range(1, 4):
        assert key in ecol
        edge = ecol.get(key)
        assert edge['_key'] == str(key)

    with pytest.raises(DocumentInsertError):
        ecol.insert_many([edge1, edge2], halt_on_error=True)

    result = ecol.insert_many([edge1, edge2], halt_on_error=False)
    assert result['created'] == 0
    assert result['errors'] == 2
    assert 'details' in result

    result = ecol.insert_many([edge4], details=False)
    assert result['created'] == 1
    assert result['errors'] == 0
    assert 'details' not in result


def test_get():
    ecol.insert(edge1)
    edge = ecol.get('1')
    assert edge['_key'] == '1'
    assert edge['_from'] == edge1['_from']
    assert edge['_to'] == edge1['_to']
    assert ecol.get('2') is None

    old_rev = edge['_rev']
    new_rev = str(int(old_rev) + 1)
    assert ecol.get('1', rev=old_rev) == edge

    with pytest.raises(DocumentRevisionError):
        ecol.get('1', rev=new_rev)


def test_get_many():
    assert ecol.get_many(['1', '2', '3', '4', '5']) == []
    expected = [edge1, edge2, edge3, edge4]
    ecol.insert_many(expected)
    assert ecol.get_many([]) == []
    assert expected == [
        {'_key': edge['_key'], '_from': edge['_from'], '_to': edge['_to']}
        for edge in ecol.get_many(['1', '2', '3', '4'])
    ]
    assert expected == [
        {'_key': edge['_key'], '_from': edge['_from'], '_to': edge['_to']}
        for edge in ecol.get_many(['1', '2', '3', '4', '5', '6'])
    ]


def test_update():
    ecol.insert(edge1)
    edge = ecol.update('1', {'value': 200})
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['value'] == 200

    edge = ecol.update('1', {'value': None}, keep_none=True)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['value'] is None

    edge = ecol.update('1', {'value': {'bar': 1}}, sync=True)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['value'] == {'bar': 1}

    edge = ecol.update('1', {'value': {'baz': 2}}, merge=True)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert ecol['1']['value'] == {'bar': 1, 'baz': 2}

    edge = ecol.update('1', {'value': None}, keep_none=False)
    assert edge['_id'] == '{}/1'.format(ecol.name)
    assert edge['_key'] == '1'
    assert 'value' not in ecol['1']

    old_rev = edge['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        ecol.update('1', {'value': 300, '_rev': new_rev})
    assert 'value' not in ecol['1']

    with pytest.raises(DocumentUpdateError):
        ecol.update('2', {'value': 300})
    assert 'value' not in ecol['1']

    ecol.update('1', {'_to': '{}/3'.format(col)})
    assert ecol['1']['_to'] == '{}/3'.format(col)

    ecol.update('1', {'_from': '{}/2'.format(col)})
    assert ecol['1']['_from'] == '{}/2'.format(col)


def test_replace():
    ecol.insert(edge1)
    new_data = {
        '_from': edge1['_from'],
        '_to': edge1['_to'],
        'value': 200
    }
    new_edge = ecol.replace('1', new_data)
    assert new_edge['_id'] == '{}/1'.format(ecol.name)
    assert new_edge['_key'] == '1'
    assert ecol['1']['_from'] == edge1['_from']
    assert ecol['1']['_to'] == edge1['_to']
    assert ecol['1']['value'] == 200

    new_data['value'] = 300
    new_edge = ecol.replace('1', new_data, sync=True)
    assert new_edge['_id'] == '{}/1'.format(ecol.name)
    assert new_edge['_key'] == '1'
    assert ecol['1']['value'] == 300

    new_data['value'] = 400
    new_edge = ecol.replace('1', new_data, rev=new_edge['_rev'])
    assert new_edge['_id'] == '{}/1'.format(ecol.name)
    assert new_edge['_key'] == '1'
    assert ecol['1']['value'] == 400

    old_rev = new_edge['_rev']
    new_rev = str(int(old_rev) + 1)

    new_data['value'] = 500
    new_data['_rev'] = new_rev
    with pytest.raises(DocumentRevisionError):
        ecol.replace('1', new_data)
    assert ecol['1']['value'] == 400

    new_data['value'] = 600
    del new_data['_rev']
    with pytest.raises(DocumentReplaceError):
        ecol.replace('2', new_data)
    assert ecol['1']['value'] == 400


def test_delete():
    ecol.insert_many([edge1, edge2, edge3])

    edge = ecol.delete('1')
    assert edge['id'] == '{}/1'.format(ecol.name)
    assert edge['key'] == '1'
    assert '1' not in ecol
    assert len(ecol) == 2

    edge = ecol.delete('2', sync=True)
    assert edge['id'] == '{}/2'.format(ecol.name)
    assert edge['key'] == '2'
    assert '2' not in ecol
    assert len(ecol) == 1

    old_rev = ecol['3']['_rev']
    new_rev = str(int(old_rev) + 1)

    with pytest.raises(DocumentRevisionError):
        ecol.delete('3', rev=new_rev)
    assert '3' in ecol
    assert len(ecol) == 1

    assert ecol.delete('4') == False
    with pytest.raises(DocumentDeleteError):
        ecol.delete('4', ignore_missing=False)
    assert len(ecol) == 1


def test_delete_many():
    result = ecol.delete_many(['1', '2', '3'])
    assert result['removed'] == 0
    assert result['ignored'] == 3

    ecol.insert_many([edge1, edge2, edge3])
    result = ecol.delete_many([])
    assert result['removed'] == 0
    assert result['ignored'] == 0
    for key in ['1', '2', '3']:
        assert key in ecol

    result = ecol.delete_many(['1'])
    assert result['removed'] == 1
    assert result['ignored'] == 0
    assert '1' not in ecol
    assert len(ecol) == 2

    result = ecol.delete_many(['4'])
    assert result['removed'] == 0
    assert result['ignored'] == 1
    assert '2' in ecol and '3' in ecol
    assert len(ecol) == 2

    result = ecol.delete_many(['1', '2', '3'])
    assert result['removed'] == 2
    assert result['ignored'] == 1
    assert len(ecol) == 0


def test_all():
    assert len(list(ecol.all())) == 0
    inserted = [edge1, edge2, edge3, edge4]
    ecol.insert_many(inserted)
    # for doc in inserted:
    #     ecol.insert(doc)
    fetched = list(ecol.all())
    assert len(fetched) == len(inserted)
    for edge in fetched:
        assert {
            '_key': edge['_key'],
            '_from': edge['_from'],
            '_to': edge['_to']
        } in inserted

    # TODO ordering seems strange
    assert len(list(ecol.all(offset=4))) == 0
    fetched = list(ecol.all(offset=2))
    assert len(fetched) == 2

    # TODO ordering seems strange
    assert len(list(ecol.all(limit=0))) == 0
    fetched = list(ecol.all(limit=2))
    assert len(fetched) == 2


def test_random():
    assert len(list(ecol.all())) == 0
    inserted = [edge1, edge2, edge3, edge4]
    ecol.insert_many(inserted)
    for attempt in range(10):
        edge = ecol.random()
        assert {
            '_key': edge['_key'],
            '_from': edge['_from'],
            '_to': edge['_to']
        } in inserted


def test_find():
    assert list(ecol.find({'value': 100})) == []
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['value'], e2['value'], e3['value'], e4['value'] = 100, 100, 200, 300
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)

    found = list(ecol.find({'value': 100}))
    assert len(found) == 2
    for edge in found:
        assert edge['_key'] in ['1', '2']
        assert {
            '_key': edge['_key'],
            '_from': edge['_from'],
            '_to': edge['_to'],
            'value': edge['value']
        } in inserted

    found = list(ecol.find({'value': 100}, offset=1))
    assert len(found) == 1
    for edge in found:
        assert edge['_key'] == '2'
        assert {
            '_key': edge['_key'],
            '_from': edge['_from'],
            '_to': edge['_to'],
            'value': edge['value']
        } in inserted

    found = list(ecol.find({}, limit=4))
    assert len(found) == 4
    for edge in found:
        assert edge['_key'] in ['1', '2', '3', '4']
        assert {
            '_key': edge['_key'],
            '_from': edge['_from'],
            '_to': edge['_to'],
            'value': edge['value']
        } in inserted

    found = list(ecol.find({'value': 200}))
    assert len(found) == 1
    assert found[0]['_key'] == '3'


def test_find_and_update():
    assert ecol.find_and_update({'value': 100}, {'bar': 100}) == 0
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['value'], e2['value'], e3['value'], e4['value'] = 100, 100, 200, 300
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)

    assert ecol.find_and_update({'value': 100}, {'new_value': 200}) == 2
    for key in ['1', '2']:
        assert ecol[key]['value'] == 100
        assert ecol[key]['new_value'] == 200

    assert ecol.find_and_update({'value': 200}, {'new_value': 100}) == 1
    assert ecol['3']['value'] == 200
    assert ecol['3']['new_value'] == 100

    assert ecol.find_and_update(
        {'value': 300},
        {'value': None},
        sync=True,
        keep_none=True
    ) == 1
    assert ecol['4']['value'] is None
    assert ecol.find_and_update(
        {'value': 200},
        {'value': None},
        sync=True,
        keep_none=False
    ) == 1
    assert 'value' not in ecol['3']
    assert 'new_value' in ecol['3']


def test_find_and_replace():
    assert ecol.find_and_replace({'value': 100}, {'bar': 100}) == 0
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['value'], e2['value'], e3['value'], e4['value'] = 100, 100, 200, 300
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)

    assert ecol.find_and_replace(
        {'value': 200},
        {'_from': e3['_from'], '_to': e3['_to'], 'new_value': 100}
    ) == 1
    assert 'value' not in ecol['3']
    assert ecol['3']['new_value'] == 100

    assert ecol.find_and_replace(
        {'value': 100},
        {'_from': e1['_from'], '_to': e1['_to'], 'new_value': 400}
    ) == 2
    for key in ['1', '2']:
        assert 'value' not in ecol[key]
        assert ecol[key]['new_value'] == 400

    assert ecol.find_and_replace(
        {'value': 500},
        {'_from': e2['_from'], '_to': e2['_to'], 'new_value': 500}
    ) == 0
    for key in ['1', '2', '3', '4']:
        assert ecol[key].get('new_value', None) != 500

    assert ecol['4']['value'] == 300
    assert 'new_value' not in ecol['4']


def test_find_and_delete():
    assert ecol.find_and_delete({'value': 100}) == 0
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['value'], e2['value'], e3['value'], e4['value'] = 100, 100, 200, 300
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)

    assert '3' in ecol
    assert ecol.find_and_delete({'value': 200}) == 1
    assert '3' not in ecol

    assert '4' in ecol
    assert ecol.find_and_delete({'value': 300}, sync=True) == 1
    assert '4' not in ecol

    assert ecol.find_and_delete({'value': 100}, limit=1) == 1
    count = 0
    for key in ['1', '2']:
        if key in ecol:
            assert ecol[key]['value'] == 100
            count += 1
    assert count == 1


def test_find_near():
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['coordinates'] = [1, 1]
    e2['coordinates'] = [2, 2]
    e3['coordinates'] = [3, 3]
    e4['coordinates'] = [4, 4]
    inserted = [e1, e4, e2, e3]
    ecol.insert_many(inserted)

    result = ecol.find_near(latitude=1, longitude=1, limit=2)
    assert clean_keys(list(result)) == [e1, e2]

    result = ecol.find_near(latitude=4, longitude=4)
    assert clean_keys(list(result)) == [e4, e3, e2, e1]


def test_find_in_range():
    ecol.add_skiplist_index(['value'])
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['value'], e2['value'], e3['value'], e4['value'] = 1, 2, 3, 4
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)
    result = ecol.find_in_range(
        field='value',
        lower=2,
        upper=4,
        offset=1,
        limit=2,
        include=True
    )
    assert clean_keys(list(result)) == [e3, e4]


# TODO the WITHIN geo function does not seem to work properly
def test_find_in_radius():
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['coordinates'] = [1, 1]
    e2['coordinates'] = [1, 4]
    e3['coordinates'] = [4, 1]
    e4['coordinates'] = [4, 4]
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)
    result = list(ecol.find_in_radius(3, 3, 10, 'distance'))
    for doc in result:
        assert 'distance' in doc


def test_find_in_square():
    e1, e2, e3, e4 = edge1.copy(), edge2.copy(), edge3.copy(), edge4.copy()
    e1['coordinates'] = [1, 1]
    e2['coordinates'] = [1, 5]
    e3['coordinates'] = [5, 1]
    e4['coordinates'] = [5, 5]
    inserted = [e1, e2, e3, e4]
    ecol.insert_many(inserted)

    result = ecol.find_in_box(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3
    )
    assert clean_keys(list(result)) == [e3, e1]

    result = ecol.find_in_box(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3,
        limit=1
    )
    assert clean_keys(list(result)) == [e3]

    result = ecol.find_in_box(
        latitude1=0,
        longitude1=0,
        latitude2=6,
        longitude2=3,
        skip=1
    )
    assert clean_keys(list(result)) == [e1]


def test_find_text():
    ecol.add_fulltext_index(['text'])
    e1, e2, e3 = edge1.copy(), edge2.copy(), edge3.copy()
    e1['text'] = 'foo'
    e2['text'] = 'bar'
    e3['text'] = 'baz'
    inserted = [e1, e2, e3]
    ecol.insert_many(inserted)
    result = ecol.find_text(
        field='text', query='foo,|bar'
    )
    assert clean_keys(list(result)) == [e1, e2]

    # Bad parameter
    with pytest.raises(DocumentFindTextError):
        ecol.find_text(field='text', query='+')

    with pytest.raises(DocumentFindTextError):
        ecol.find_text(field='text', query='|')


def test_list_indexes():
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'primary',
        'fields': ['_key'],
        'unique': True
    }
    indexes = ecol.indexes()
    assert isinstance(indexes, dict)
    assert expected_index in indexes.values()


def test_add_hash_index():
    ecol.add_hash_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'selectivity': 1,
        'sparse': False,
        'type': 'hash',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in ecol.indexes().values()


def test_add_skiplist_index():
    ecol.add_skiplist_index(['attr1', 'attr2'], unique=True)
    expected_index = {
        'sparse': False,
        'type': 'skiplist',
        'fields': ['attr1', 'attr2'],
        'unique': True
    }
    assert expected_index in ecol.indexes().values()


def test_add_geo_index():
    # With one attribute
    ecol.add_geo_index(
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
    assert expected_index in ecol.indexes().values()

    # With two attributes
    ecol.add_geo_index(
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
    assert expected_index in ecol.indexes().values()

    # With more than two attributes (should fail)
    with pytest.raises(IndexCreateError):
        ecol.add_geo_index(fields=['attr1', 'attr2', 'attr3'])


def test_add_fulltext_index():
    # With two attributes (should fail)
    with pytest.raises(IndexCreateError):
        ecol.add_fulltext_index(fields=['attr1', 'attr2'])

    ecol.add_fulltext_index(
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
    assert expected_index in ecol.indexes().values()


def test_delete_index():
    old_indexes = set(ecol.indexes())
    ecol.add_hash_index(['attr1', 'attr2'], unique=True)
    ecol.add_skiplist_index(['attr1', 'attr2'], unique=True)
    ecol.add_fulltext_index(fields=['attr1'], min_length=10)

    new_indexes = set(ecol.indexes())
    assert new_indexes.issuperset(old_indexes)

    for index_id in new_indexes - old_indexes:
        ecol.delete_index(index_id)
    assert set(ecol.indexes()) == old_indexes
