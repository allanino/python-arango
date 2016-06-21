from __future__ import absolute_import, unicode_literals

import pytest

from arango import Connection
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_col_name
)


conn = Connection()

db_name = generate_db_name(conn)
db = conn.create_database(db_name)
col_name = generate_col_name(db)
db.create_collection(col_name)
func_name = ''
func_body = ''


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)


@pytest.mark.order1
def test_query_explain():
    fields_to_check = [
        'estimatedNrItems',
        'estimatedCost',
        'rules',
        'variables',
        'collections',
    ]

    # Test invalid query
    with pytest.raises(QueryExplainError):
        db.query.explain('THIS IS AN INVALID QUERY')

    # Test valid query (all_plans=True)
    plans = db.query.explain(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=True,
        optimizer_rules=["-all", "+use-index-range"]
    )
    for plan in plans:
        for field in fields_to_check:
            assert field in plan

    # Test valid query (all_plans=False)
    plan = db.query.explain(
        "FOR d IN {} RETURN d".format(col_name),
        all_plans=False,
        optimizer_rules=["-all", "+use-index-range"]
    )
    for field in fields_to_check:
        assert field in plan


@pytest.mark.order2
def test_query_validate():
    # Test invalid query
    with pytest.raises(QueryValidateError):
        db.query.validate('THIS IS AN INVALID QUERY')

    # Test valid query
    result = db.query.validate("FOR d IN {} RETURN d".format(col_name))
    assert 'ast' in result
    assert 'bindVars' in result
    assert 'collections' in result
    assert 'parsed' in result


@pytest.mark.order3
def test_query_execute():
    # Test invalid AQL query
    with pytest.raises(AQLQueryExecuteError):
        db.query.execute('THIS IS AN INVALID QUERY')

    # Test valid AQL query #1
    db.collection(col_name).insert_bulk([
        {"_key": "doc01"},
        {"_key": "doc02"},
        {"_key": "doc03"},
    ])
    result = db.query.execute(
        "FOR d IN {} RETURN d".format(col_name),
        count=True,
        batch_size=1,
        ttl=10,
        optimizer_rules=["+all"]
    )
    assert set(d['_key'] for d in result) == {'doc01', 'doc02', 'doc03'}

    # Test valid AQL query #2
    db.collection(col_name).insert_bulk([
        {"_key": "doc04", "value": 1},
        {"_key": "doc05", "value": 1},
        {"_key": "doc06", "value": 3},
    ])
    result = db.query.execute(
        "FOR d IN {} FILTER d.value == @value RETURN d".format(col_name),
        bind_vars={'value': 1}
    )
    assert set(d['_key'] for d in result) == {'doc04', 'doc05'}


@pytest.mark.order4
def test_query_function_create_and_list():
    global func_name, func_body

    assert db.query.functions() == {}
    func_name = 'myfunctions::temperature::celsiustofahrenheit'
    func_body = 'function (celsius) { return celsius * 1.8 + 32; }'

    # Test create AQL function
    db.query.create_function(func_name, func_body)
    assert db.query.functions() == {func_name: func_body}

    # Test create AQL function again (idempotency)
    db.query.create_function(func_name, func_body)
    assert db.query.functions() == {func_name: func_body}

    # Test create invalid AQL function
    func_body = 'function (celsius) { invalid syntax }'
    with pytest.raises(AQLFunctionCreateError):
        result = db.query.create_function(func_name, func_body)
        assert result is True


@pytest.mark.order5
def test_query_function_delete_and_list():
    # Test delete AQL function
    result = db.query.delete_function(func_name)
    assert result is True

    # Test delete missing AQL function
    with pytest.raises(AQLFunctionDeleteError):
        db.query.delete_function(func_name)

    # Test delete missing AQL function (ignore_missing)
    result = db.query.delete_function(func_name, ignore_missing=True)
    assert result is False
    assert db.query.functions() == {}


@pytest.mark.order6
def test_get_query_cache_options():
    options = db.query.cache.properties()
    assert 'mode' in options
    assert 'limit' in options


@pytest.mark.order7
def test_set_query_cache_options():
    options = db.query.cache.set_properties(
        mode='on', limit=100
    )
    assert options['mode'] == 'on'
    assert options['limit'] == 100

    options = db.query.cache.properties()
    assert options['mode'] == 'on'
    assert options['limit'] == 100


@pytest.mark.order8
def test_clear_query_cache():
    result = db.query.cache.clear()
    assert isinstance(result, bool)
