from __future__ import unicode_literals

from arango.async import AsyncExecution
from arango.batch import BatchExecution
from arango.collection import Collection
from arango.constants import HTTP_OK
from arango.exceptions import *
from arango.graph import Graph
from arango.transaction import Transaction
from arango.query import Query


class Database(object):
    """ArangoDB database object.

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    """

    def __init__(self, connection):
        self._conn = connection
        self._query = Query(self._conn)

    def __repr__(self):
        return '<ArangoDB database "{}">'.format(self._conn.database)

    def __getitem__(self, name):
        return self.collection(name)

    @property
    def name(self):
        """Return the name of the database.

        :returns: the name of the database
        :rtype: str
        """
        return self._conn.database

    @property
    def query(self):
        """Return the database query object.

        AQL statements can be executed through this object.

        :returns: the database query object
        :rtype: arango.query.Query
        """
        return self._query

    def async(self, return_result=True):
        """Return the async execution object.

        API requests via async execution objects are placed in a server-side,
        in-memory task queue and executed asynchronously in a fire-and-forget
        style.

        If ``return_result`` is set to True, an AsyncJob instance is returned
        each time a request is issued through the async execution objects.
        AsyncJob objects can be used to keep track of the status of the request
        and retrieve the result.

        :param return_result: whether to store and return the result
        :type return_result: bool
        :return: the async execution object
        :rtype: arango.async.AsyncExecution
        """
        return AsyncExecution(self._conn, return_result)

    def batch(self, return_result=True):
        """Return the batch execution object.

        API requests via batch execution objects are placed in an in-memory
        queue inside the object and committed as a whole in a single call.

        If ``return_result`` is set to True, a BatchJob instance is returned
        each time a request is issued. BatchJob objects can be used to retrieve
        the result of the corresponding request after the commit.

        :param return_result: whether to store and return the result
        :type return_result: bool
        :return: the batch execution object
        rtype: arango.batch.BatchExecution
        """
        return BatchExecution(self._conn, return_result)

    def transaction(self, return_result=True):
        return Transaction(self._conn, return_result)

    def properties(self):
        """Return the database properties.

        :returns: the database properties
        :rtype: dict
        :raises: DatabasePropertiesGetError
        """
        res = self._conn.get('/_api/database/current')
        if res.status_code not in HTTP_OK:
            raise DatabasePropertiesGetError(res)
        result = res.body['result']
        result['system'] = result.pop('isSystem')
        return result

    #########################
    # Collection Management #
    #########################

    def list_collections(self):
        """Return the names of the collections in the database.

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
                'type': Collection.TYPES[col['type']],
                'status': Collection.STATUSES[col['status']],
            }
            for col in res.body['result']
        }

    def collection(self, name):
        """Return a collection object.

        :param name: the name of the collection
        :type name: str
        :returns: the collection object
        :rtype: arango.collection.Collection
        """
        return Collection(self._conn, name)

    def c(self, name):
        """Alias for self.collection."""
        return self.collection(name)

    def create_collection(self, name, sync=False, compact=True, system=False,
                          journal_size=None, edge=False, volatile=False,
                          user_keys=True, key_increment=None, key_offset=None,
                          key_generator="traditional", shard_fields=None,
                          shard_count=None):
        """Create a collection.

        :param name: the name of the collection
        :type name: str
        :param sync: wait for the operation to sync to disk
        :type sync: bool
        :param compact: compact the collection
        :type compact: bool
        :param system: the collection is a system collection
        :type system: bool
        :param journal_size: the max size of the journal
        :type journal_size: int
        :param edge: the collection is an edge collection
        :type edge: bool
        :param volatile: the collection is in-memory only
        :type volatile: bool
        :param key_generator: "traditional" or "autoincrement"
        :type key_generator: str
        :param user_keys: whether to allow users to supply keys
        :type user_keys: bool
        :param key_increment: the increment value (autoincrement only)
        :type key_increment: int
        :param key_offset: the offset value (autoincrement only)
        :type key_offset: int
        :param shard_fields: the field(s) used to determine the target shard
        :type shard_fields: list
        :param shard_count: the number of shards to create
        :type shard_count: int
        :raises: CollectionCreateError
        :returns: the new collection object
        :rtype: arango.collection.Collection
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

    def delete_collection(self, name, ignore_missing=False):
        """Delete a collection.

        :param name: the name of the collection to delete
        :type name: str
        :param ignore_missing: do not raise if the collection is missing
        :type ignore_missing: bool
        :returns: whether the deletion was successful
        :rtype: bool
        :raises: CollectionDeleteError
        """
        res = self._conn.delete('/_api/collection/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise CollectionDeleteError(res)
        return not res.body['error']

    ####################
    # Graph Management #
    ####################

    def list_graphs(self):
        """List all graphs in the database.

        :returns: the graphs in the database
        :rtype: dict
        :raises: GraphGetError
        """
        res = self._conn.get('/_api/gharial')
        if res.status_code not in HTTP_OK:
            raise GraphListError(res)
        return [
            {
                'name': graph['_key'],
                'revision': graph['_rev'],
                'edge_definitions': graph['edgeDefinitions'],
                'orphan_collections': graph['orphan_collections']
            } for graph in res.body['graphs']
        ]

    def graph(self, name):
        """Return the Graph object of the specified name.

        :param name: the name of the graph
        :type name: str
        :returns: the requested graph object
        :rtype: arango.graph.Graph
        :raises: TypeError, GraphNotFound
        """
        return Graph(self._conn, name)

    def g(self, name):
        """Alias for self.graph."""
        return self.graph(name)

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

    def delete_graph(self, name, ignore_missing=False):
        """Drop the graph of the given name from this database.

        :param name: the name of the graph to delete
        :type name: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: whether the drop was successful
        :rtype: bool
        :raises: GraphDeleteError
        """
        res = self._conn.delete('/_api/gharial/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise GraphDeleteError(res)
        return not res.body['error']
