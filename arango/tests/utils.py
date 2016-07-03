from __future__ import absolute_import, unicode_literals

from random import randint
from collections import Mapping, Iterable


def generate_db_name(client):
    """Generate and return the next available database name.

    :param client: ArangoDB client
    :type client: arango.ArangoClient
    :returns: the next available database name
    :rtype: str
    """
    num = randint(100000, 999999)
    existing = set(client.databases())
    while "test_database_{num:06d}".format(num=num) in existing:
        num = randint(100000, 999999)
    return "test_database_{num:06d}".format(num=num)


def generate_col_name(database):
    """Generate and return the next available collection name.

    :param database: ArangoDB database
    :type database: arango.database.ArangoDatabase
    :returns: the next available collection name
    :rtype: str
    """
    num = randint(100000, 999999)
    existing = set(database.collections())
    while "test_collection_{num:06d}".format(num=num) in existing:
        num = randint(100000, 999999)
    return "test_collection_{num:06d}".format(num=num)


def generate_graph_name(database):
    """Generate and return the next available collection name.

    :param database: ArangoDB database
    :type database: arango.database.ArangoDatabase
    :returns: the next available graph name
    :rtype: str
    """
    num = randint(100000, 999999)
    existing = set(database.graphs())
    while "test_graph_{num:06d}".format(num=num) in existing:
        num = randint(100000, 999999)
    return "test_graph_{num:06d}".format(num=num)


def generate_task_name(client):
    """Generate and return the next available task name.

    :param client: ArangoDB client
    :type client: arango.ArangoClient
    :returns: the next available database name
    :rtype: str
    """
    num = randint(100000, 999999)
    existing = set(task['name'] for task in client.tasks().values())
    while "test_task_{num:06d}".format(num=num) in existing:
        num = randint(100000, 999999)
    return "test_task_{num:06d}".format(num=num)


def generate_user_name(client):
    """Generate and return the next available user name.

    :param client: ArangoDB client
    :type client: arango.ArangoClient
    :returns: the next available database name
    :rtype: str
    """
    num = randint(100000, 999999)
    existing = set(client.users())
    while "test_user_{num:06d}".format(num=num) in existing:
        num = randint(100000, 999999)
    return "test_user_{num:06d}".format(num=num)


def clean_keys(obj):
    """Return the document(s) with all the system keys stripped.

    :param obj: document(s)
    :type obj: list or dict
    :returns: the document(s) with the system keys stripped
    :rtype: list or dict
    """
    if isinstance(obj, Mapping):
        return {
            k: v for k, v in obj.items()
            if not (k not in {'_key', '_from', '_to'} and k.startswith("_"))
        }
    elif isinstance(obj, Iterable):
        return [{
            k: v for k, v in document.items()
            if not (k not in {'_key', '_from', '_to'} and k.startswith("_"))
        } for document in obj]