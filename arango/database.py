from __future__ import unicode_literals

from copy import deepcopy

from arango.batch import Batch
from arango.collection import Collection
from arango.constants import (
    HTTP_OK,
    COLLECTION_STATUSES,
    COLLECTION_TYPES
)
from arango.exceptions import *
from arango.graph import Graph
from arango.transaction import Transaction
from arango.query import Query


class Database(object):
    """Wrapper for ArangoDB's database-specific API which cover:

    1. Database properties
    2. Collection Management
    3. Graph Management

    :param connection: ArangoDB API connection object
    :type connection: arango.connection.Connection
    """

    def __init__(self, connection, name):
        connection = deepcopy(connection)
        connection.set_db(name)
        self._name = name
        self._conn = connection

    def __repr__(self):
        """Return a descriptive string of this instance."""
        return "<ArangoDB database '{}'>".format(self._name)

    def __getitem__(self, name):
        """Return the collection of the given name."""
        return Collection(self._conn, name)

    @property
    def name(self):
        """Return the name of the database.

        :returns: the name of the database
        :rtype: str
        """
        return self._name

    @property
    def query(self):
        return Query(self._conn)

    def batch(self, return_result=True):
        return Batch(self._conn, return_result)

    def transaction(self):
        return Transaction(self._conn)

    def options(self):
        """Return all properties of this database.

        :returns: the database properties
        :rtype: dict
        :raises: DatabaseOptionsGetError
        """
        res = self._conn.get('/_api/database/current')
        if res.status_code not in HTTP_OK:
            raise DatabaseOptionsGetError(res)
        result = res.body['result']
        result['system'] = result.pop('isSystem')
        return result

    #########################
    # Collection Management #
    #########################

    def collections(self):
        """Return the names of the collections in this database.

        :returns: the names of the collections
        :rtype: dict
        :raises: CollectionListError
        """
        res = self._conn.get('/_api/collection')
        if res.status_code not in HTTP_OK:
            raise CollectionListError(res)
        return {
            col['name']: {
                'id': col['id'],
                'system': col['isSystem'],
                'type': COLLECTION_TYPES[col['type']],
                'status': COLLECTION_STATUSES[col['status']],
            }
            for col in res.body['result']
        }

    def collection(self, name):
        """Return the Collection object of the specified name.

        :param name: the name of the collection
        :type name: str
        :returns: the requested collection object
        :rtype: arango.collection.Collection
        :raises: TypeError
        """
        return Collection(self._conn, name)

    def create_collection(self, name, sync=False, compact=True, system=False,
                          journal_size=None, edge=False, volatile=False,
                          user_keys=True, key_increment=None, key_offset=None,
                          key_generator="traditional", shard_fields=None,
                          shard_count=None):
        """Create a new collection to this database.

        :param name: name of the new collection
        :type name: str
        :param sync: wait for the create to sync to disk
        :type sync: bool
        :param compact: whether or not the collection is compacted
        :type compact: bool
        :param system: whether or not the collection is a system collection
        :type system: bool
        :param journal_size: the max size of the journal or datafile
        :type journal_size: int
        :param edge: whether or not the collection is an edge collection
        :type edge: bool
        :param volatile: whether or not the collection is in-memory only
        :type volatile: bool
        :param key_generator: ``traditional`` or ``autoincrement``
        :type key_generator: str
        :param user_keys: whether to allow users to supply keys
        :type user_keys: bool
        :param key_increment: increment value for ``autoincrement`` generator
        :type key_increment: int
        :param key_offset: initial offset value for ``autoincrement`` generator
        :type key_offset: int
        :param shard_fields: the field(s) used to determine the target shard
        :type shard_fields: list
        :param shard_count: the number of shards to create
        :type shard_count: int
        :raises: CollectionCreateError
        :returns: the created collection
        :rtype: Collection | EdgeCollection
        """
        key_options = {
            'type': key_generator,
            'allowUserKeys': user_keys
        }
        if key_increment is not None:
            key_options['increment'] = key_increment
        if key_offset is not None:
            key_options['offset'] = key_offset
        data = {
            'name': name,
            'waitForSync': sync,
            'doCompact': compact,
            'isSystem': system,
            'isVolatile': volatile,
            'type': 3 if edge else 2,
            'keyOptions': key_options
        }
        if journal_size is not None:
            data['journalSize'] = journal_size
        if shard_count is not None:
            data['numberOfShards'] = shard_count
        if shard_fields is not None:
            data['shardKeys'] = shard_fields

        res = self._conn.post('/_api/collection', data=data)
        if res.status_code not in HTTP_OK:
            raise CollectionCreateError(res)
        return self.collection(name)

    def drop_collection(self, name, ignore_missing=False):
        """Drop the specified collection from this database.

        :param name: the name of the collection to delete
        :type name: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: whether the deletion was successful
        :rtype: bool
        :raises: CollectionDropError
        """
        res = self._conn.delete('/_api/collection/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise CollectionDropError(res)
        return not res.body['error']

    ####################
    # Graph Management #
    ####################

    def graphs(self):
        """List all graphs in this database.

        :returns: the graphs in this database
        :rtype: dict
        :raises: GraphGetError
        """
        res = self._conn.get('/_api/gharial')
        if res.status_code not in HTTP_OK:
            raise GraphListError(res)
        return [graph['_key'] for graph in res.body['graphs']]

    def graph(self, name):
        """Return the Graph object of the specified name.

        :param name: the name of the graph
        :type name: str
        :returns: the requested graph object
        :rtype: arango.graph.Graph
        :raises: TypeError, GraphNotFound
        """
        return Graph(self._conn, name)

    def create_graph(self, name, edge_definitions=None,
                     orphan_collections=None):
        """Create a new graph in this database.

        # TODO expand on edge_definitions and orphan_collections

        :param name: name of the new graph
        :type name: str
        :param edge_definitions: definitions for edges
        :type edge_definitions: list
        :param orphan_collections: names of additional vertex collections
        :type orphan_collections: list
        :returns: the graph object
        :rtype: arango.graph.Graph
        :raises: GraphCreateError
        """
        data = {'name': name}
        if edge_definitions is not None:
            data['edgeDefinitions'] = edge_definitions
        if orphan_collections is not None:
            data['orphanCollections'] = orphan_collections

        res = self._conn.post('/_api/gharial', data=data)
        if res.status_code not in HTTP_OK:
            raise GraphCreateError(res)
        return Graph(self._conn, name)

    def drop_graph(self, name, ignore_missing=False):
        """Drop the graph of the given name from this database.

        :param name: the name of the graph to delete
        :type name: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: whether the drop was successful
        :rtype: bool
        :raises: GraphDropError
        """
        res = self._conn.delete('/_api/gharial/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise GraphDropError(res)
        return not res.body['error']
