from __future__ import absolute_import, unicode_literals

import random
from datetime import datetime

import pytest
from six import string_types

from arango.connection import Connection
from arango.database import Database
from arango.exceptions import *
from arango.tests.utils import (
    generate_db_name,
    generate_user_name,
    generate_task_name
)

conn = Connection()
db_name = generate_db_name(conn)
username = generate_user_name(conn)
task_name = generate_task_name(conn)
task_id = ''


def teardown_module(*_):
    conn.drop_database(db_name, ignore_missing=True)
    conn.delete_user(username, ignore_missing=True)
    if task_id:
        conn.delete_task(task_id, ignore_missing=True)


def test_properties():
    assert conn.protocol == 'http'
    assert conn.host == 'localhost'
    assert conn.port == 8529
    assert 'ArangoDB connection at' in repr(conn)


def test_version():
    version = conn.version()
    assert isinstance(version, string_types)


def test_details():
    details = conn.details()
    assert 'architecture' in details
    assert 'server-version' in details


def test_required_db_version():
    version = conn.required_db_version()
    assert isinstance(version, string_types)


def test_statistics():
    statistics = conn.statistics(description=False)
    assert isinstance(statistics, dict)
    assert 'time' in statistics
    assert 'system' in statistics
    assert 'server' in statistics

    description = conn.statistics(description=True)
    assert isinstance(description, dict)
    assert 'figures' in description
    assert 'groups' in description


def test_role():
    assert conn.role() in {
        'SINGLE',
        'COORDINATOR',
        'PRIMARY',
        'SECONDARY',
        'UNDEFINED'
    }


def test_time():
    system_time = conn.time()
    assert isinstance(system_time, datetime)


def test_echo():
    last_request = conn.echo()
    assert 'protocol' in last_request
    assert 'user' in last_request
    assert 'requestType' in last_request
    assert 'rawRequestBody' in last_request


def test_sleep():
    assert conn.sleep(2) == 2


# def test_shutdown():
#     assert isinstance(conn.shutdown(), bool)


# def test_run_tests():
#     assert isinstance(conn.run_tests, dict)


def test_execute():
    assert conn.execute('return 1') == '1'
    assert conn.execute('return "test"') == '"test"'
    with pytest.raises(ProgramExecuteError) as err:
        conn.execute('return invalid')
    assert err.value.message == 'Internal Server Error'


# TODO test parameters
def test_log():
    log = conn.read_log()
    assert 'lid' in log
    assert 'level' in log
    assert 'text' in log
    assert 'total_amount' in log


def test_reload_routing():
    result = conn.reload_routing()
    assert isinstance(result, bool)


def test_endpoints():
    endpoints = conn.endpoints()
    assert isinstance(endpoints, list)
    for endpoint in endpoints:
        assert 'endpoint' in endpoint


def test_database_management():
    # Test list databases
    # TODO something wrong here
    result = conn.databases()
    assert '_system' in result

    result = conn.databases(user_only=True)
    assert '_system' in result

    assert db_name not in conn.databases()

    # Test create database
    result = conn.create_database(db_name)
    assert isinstance(result, Database)
    assert db_name in conn.databases()

    # Test get after create database
    assert isinstance(conn.db(db_name), Database)
    assert conn.db(db_name).name == db_name

    # Test create duplicate database
    with pytest.raises(DatabaseCreateError):
        conn.create_database(db_name)

    # Test list after create database
    assert db_name in conn.databases()

    # Test drop database
    result = conn.drop_database(db_name)
    assert result is True
    assert db_name not in conn.databases()

    # Test drop missing database
    with pytest.raises(DatabaseDeleteError):
        conn.drop_database(db_name)

    # Test drop missing database (ignore missing)
    result = conn.drop_database(db_name, ignore_missing=True)
    assert result is False


def test_user_management():
    # Test get users
    users = conn.users()
    assert isinstance(users, dict)
    assert 'root' in users

    root_user = users['root']
    assert 'active' in root_user
    assert 'extra'in root_user
    assert 'change_password' in root_user

    assert username not in conn.users()

    # Test create user
    user = conn.create_user(
        username,
        'password',
        active=True,
        extra={'hello': 'world'},
        change_password=False,
    )
    assert user['active'] is True
    assert user['extra'] == {'hello': 'world'}
    assert user['change_password'] is False
    assert username in conn.users()

    # Test create duplicate user
    with pytest.raises(UserCreateError):
        conn.create_user(username, 'password')

    missing = generate_user_name(conn)

    # Test update user
    user = conn.update_user(
        username,
        password='test',
        active=False,
        extra={'foo': 'bar'},
        change_password=True
    )
    assert user['active'] is False
    assert user['extra'] == {
        'hello': 'world',
        'foo': 'bar'
    }
    assert user['change_password'] is True

    # Test update missing user
    with pytest.raises(UserUpdateError):
        conn.update_user(
            missing,
            password='test',
            active=False,
            extra={'foo': 'bar'},
            change_password=True
        )

    # Test replace user
    user = conn.replace_user(
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
        conn.replace_user(
            missing,
            password='test',
            active=True,
            extra={'foo': 'baz'},
            change_password=False
        )

    # Test delete user
    result = conn.delete_user(username)
    assert result is True
    assert username not in conn.users()

    # Test delete missing user
    with pytest.raises(UserDeleteError):
        conn.delete_user(username)

    # Test delete missing user (ignore missing)
    result = conn.delete_user(username, ignore_missing=True)
    assert result is False


def test_task_management():
    global task_id

    # Test get tasks
    tasks = conn.tasks()
    assert isinstance(tasks, dict)
    for task in tasks.values():
        assert 'command' in task
        assert 'created' in task
        assert 'database' in task
        assert 'id' in task
        assert 'name' in task

    # Test get task
    tasks = conn.tasks()
    if tasks:
        chosen_task_id = random.choice(list(tasks.keys()))
        retrieved_task = conn.task(chosen_task_id)
        assert tasks[chosen_task_id] == retrieved_task

    cmd = "(function(params) { require('internal').print(params); })(params)"

    # Test create task
    assert task_name not in conn.tasks()
    task = conn.create_task(
        name=task_name,
        command=cmd,
        params={'foo': 'bar', 'bar': 'foo'},
        period=2,
        offset=3,
    )
    task_id = task['id']
    assert task_id in conn.tasks()
    assert task_name == conn.tasks()[task_id]['name']

    # Test get after create task
    task = conn.task(task_id)
    assert task['command'] == cmd
    assert task['name'] == task_name
    assert task['period'] == 2

    # Test create duplicate task (with ID)
    with pytest.raises(TaskCreateError):
        task = conn.create_task(
            task_id=task_id,
            name=task_name,
            command=cmd,
            params={'foo': 'bar', 'bar': 'foo'},
            period=3,
            offset=4,
        )

    # Test delete task
    result = conn.delete_task(task['id'])
    assert result is True
    assert task_id not in conn.tasks()

    # Test delete missing task
    with pytest.raises(TaskDeleteError):
        conn.delete_task(task['id'])

    # Test delete missing task (ignore missing)
    result = conn.delete_task(task['id'], ignore_missing=True)
    assert result is False

    # Test create task with ID
    task = conn.create_task(
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
    task = conn.task(task_id)
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
