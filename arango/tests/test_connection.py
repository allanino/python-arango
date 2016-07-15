from __future__ import absolute_import, unicode_literals

import random
from datetime import datetime

import pytest
from six import string_types

from arango import ArangoClient
from arango.database import Database
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_user_name,
    generate_task_name
)

arango_client = ArangoClient()
db_name = generate_db_name(arango_client)
username = generate_user_name(arango_client)
task_name = generate_task_name(arango_client)
task_id = ''


def teardown_module(*_):
    arango_client.delete_database(db_name, ignore_missing=True)
    arango_client.delete_user(username, ignore_missing=True)
    if task_id:
        arango_client.delete_task(task_id, ignore_missing=True)


def test_properties():
    assert arango_client.protocol == 'http'
    assert arango_client.host == 'localhost'
    assert arango_client.port == 8529
    assert 'ArangoDB client pointing to' in repr(arango_client)


def test_version():
    version = arango_client.version()
    assert isinstance(version, string_types)


def test_details():
    details = arango_client.details()
    assert 'architecture' in details
    assert 'server-version' in details


def test_required_db_version():
    version = arango_client.required_db_version()
    assert isinstance(version, string_types)


def test_statistics():
    statistics = arango_client.statistics(description=False)
    assert isinstance(statistics, dict)
    assert 'time' in statistics
    assert 'system' in statistics
    assert 'server' in statistics

    description = arango_client.statistics(description=True)
    assert isinstance(description, dict)
    assert 'figures' in description
    assert 'groups' in description


def test_role():
    assert arango_client.role() in {
        'SINGLE',
        'COORDINATOR',
        'PRIMARY',
        'SECONDARY',
        'UNDEFINED'
    }


def test_time():
    system_time = arango_client.time()
    assert isinstance(system_time, datetime)


def test_echo():
    last_request = arango_client.echo()
    assert 'protocol' in last_request
    assert 'user' in last_request
    assert 'requestType' in last_request
    assert 'rawRequestBody' in last_request


def test_sleep():
    assert arango_client.sleep(2) == 2


def test_execute():
    assert arango_client.execute('return 1') == '1'
    assert arango_client.execute('return "test"') == '"test"'
    with pytest.raises(ProgramExecuteError) as err:
        arango_client.execute('return invalid')
    assert err.value.message == 'Internal Server Error'


# TODO test parameters
def test_log():
    log = arango_client.read_log()
    assert 'lid' in log
    assert 'level' in log
    assert 'text' in log
    assert 'total_amount' in log


def test_reload_routing():
    result = arango_client.reload_routing()
    assert isinstance(result, bool)


def test_endpoints():
    endpoints = arango_client.endpoints()
    assert isinstance(endpoints, list)
    for endpoint in endpoints:
        assert 'endpoint' in endpoint


def test_database_management():
    # Test list databases
    # TODO something wrong here
    result = arango_client.list_databases()
    assert '_system' in result
    result = arango_client.list_databases(user_only=True)
    assert '_system' in result
    assert db_name not in arango_client.list_databases()

    # Test create database
    result = arango_client.create_database(db_name)
    assert isinstance(result, Database)
    assert db_name in arango_client.list_databases()

    # Test get after create database
    assert isinstance(arango_client.db(db_name), Database)
    assert arango_client.db(db_name).name == db_name

    # Test create duplicate database
    with pytest.raises(DatabaseCreateError):
        arango_client.create_database(db_name)

    # Test list after create database
    assert db_name in arango_client.list_databases()

    # Test delete database
    result = arango_client.delete_database(db_name)
    assert result is True
    assert db_name not in arango_client.list_databases()

    # Test delete missing database
    with pytest.raises(DatabaseDeleteError):
        arango_client.delete_database(db_name)

    # Test delete missing database (ignore missing)
    result = arango_client.delete_database(db_name, ignore_missing=True)
    assert result is False


def test_user_management():
    # Test get users
    users = arango_client.list_users()
    assert isinstance(users, dict)
    assert 'root' in users

    root_user = users['root']
    assert 'active' in root_user
    assert 'extra'in root_user
    assert 'change_password' in root_user

    assert username not in arango_client.list_users()

    # Test create user
    user = arango_client.create_user(
        username,
        'password',
        active=True,
        extra={'hello': 'world'},
        change_password=False,
    )
    assert user['active'] is True
    assert user['extra'] == {'hello': 'world'}
    assert user['change_password'] is False
    assert username in arango_client.list_users()

    # Test create duplicate user
    with pytest.raises(UserCreateError):
        arango_client.create_user(username, 'password')

    missing = generate_user_name(arango_client)

    # Test update user
    user = arango_client.update_user(
        username,
        password='new_password',
        active=False,
        extra={'foo': 'bar'},
        change_password=True
    )
    assert user['active'] is False
    assert user['extra'] == {'hello': 'world', 'foo': 'bar'}
    assert user['change_password'] is False

    # Test update missing user
    with pytest.raises(UserUpdateError):
        arango_client.update_user(
            missing,
            password='test',
            active=False,
            extra={'foo': 'bar'},
            change_password=True
        )

    # Test replace user
    user = arango_client.replace_user(
        username,
        password='test',
        active=True,
        extra={'foo': 'baz'},
        change_password=False
    )
    assert user['active'] is True
    assert user['extra'] == {'foo': 'baz'}
    assert user['change_password'] is False

    # Test replace missing user
    with pytest.raises(UserReplaceError):
        arango_client.replace_user(
            missing,
            password='test',
            active=True,
            extra={'foo': 'baz'},
            change_password=False
        )

    # Test revoke access
    result = arango_client.revoke_user_access(username, db_name)
    assert isinstance(result, bool)

    with pytest.raises(UserRevokeAccessError):
        arango_client.revoke_user_access(missing, db_name)

    # Test grant access
    result = arango_client.grant_user_access(username, db_name)
    assert isinstance(result, bool)

    with pytest.raises(UserGrantAccessError):
        arango_client.grant_user_access(missing, db_name)

    # Test delete user
    result = arango_client.delete_user(username)
    assert result is True
    assert username not in arango_client.list_users()

    # Test delete missing user
    with pytest.raises(UserDeleteError):
        arango_client.delete_user(username)

    # Test delete missing user (ignore missing)
    result = arango_client.delete_user(username, ignore_missing=True)
    assert result is False


def test_task_management():
    global task_id

    # Test get tasks
    tasks = arango_client.list_tasks()
    assert isinstance(tasks, dict)
    for task in tasks.values():
        assert 'command' in task
        assert 'created' in task
        assert 'database' in task
        assert 'id' in task
        assert 'name' in task

    # Test get task
    tasks = arango_client.list_tasks()
    if tasks:
        chosen_task_id = random.choice(list(tasks.keys()))
        retrieved_task = arango_client.task(chosen_task_id)
        assert tasks[chosen_task_id] == retrieved_task

    cmd = "(function(params) { require('internal').print(params); })(params)"

    # Test create task
    assert task_name not in arango_client.list_tasks()
    task = arango_client.create_task(
        name=task_name,
        command=cmd,
        params={'foo': 'bar', 'bar': 'foo'},
        period=2,
        offset=3,
    )
    task_id = task['id']
    assert task_id in arango_client.list_tasks()
    assert task_name == arango_client.list_tasks()[task_id]['name']

    # Test get after create task
    task = arango_client.task(task_id)
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 2

    # Test create duplicate task (with ID)
    with pytest.raises(TaskCreateError):
        task = arango_client.create_task(
            task_id=task_id,
            name=task_name,
            command=cmd,
            params={'foo': 'bar', 'bar': 'foo'},
            period=3,
            offset=4,
        )

    # Test delete task
    result = arango_client.delete_task(task['id'])
    assert result is True
    assert task_id not in arango_client.list_tasks()

    # Test delete missing task
    with pytest.raises(TaskDeleteError):
        arango_client.delete_task(task['id'])

    # Test delete missing task (ignore missing)
    result = arango_client.delete_task(task['id'], ignore_missing=True)
    assert result is False

    # Test create task with ID
    task = arango_client.create_task(
        task_id=task_id,
        name=task_name,
        command=cmd,
        params={'foo': 'bar', 'bar': 'foo'},
        period=3,
        offset=4,
    )
    assert task['id'] == task_id
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 3

    # Test get after create task with ID
    task = arango_client.task(task_id)
    assert task['id'] == task_id
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 3


# def test_execute_transaction():
#     # Test execute transaction with no params
#     action = """
#         function () {{
#             var db = require('internal').db;
#             db.{col}.save({{ _key: 'doc1'}});
#             db.{col}.save({{ _key: 'doc2'}});
#             return 'success!';
#         }}
#     """.format(col=col_name)
#
#     result = db.execute_transaction(
#         action=action,
#         read_collections=[col_name],
#         write_collections=[col_name],
#         sync=True,
#         lock_timeout=10000
#     )
#     assert result == 'success!'
#     assert 'doc1' in collection
#     assert 'doc2' in collection
#
#     # Test execute transaction with params
#     action = """
#         function (params) {{
#             var db = require('internal').db;
#             db.{col}.save({{ _key: 'doc3', val: params.val1 }});
#             db.{col}.save({{ _key: 'doc4', val: params.val2 }});
#             return 'success!';
#         }}
#     """.format(col=col_name)
#
#     result = db.execute_transaction(
#         action=action,
#         read_collections=[col_name],
#         write_collections=[col_name],
#         params={"val1": 1, "val2": 2},
#         sync=True,
#         lock_timeout=10000
#     )
#     assert result == 'success!'
#     assert 'doc3' in collection
#     assert 'doc4' in collection
#     assert collection["doc3"]["val"] == 1
#     assert collection["doc4"]["val"] == 2
