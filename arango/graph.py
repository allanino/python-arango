from __future__ import absolute_import, unicode_literals

from arango.collection import BaseCollection
from arango.constants import HTTP_OK
from arango.exceptions import *
from arango.request import Request
from arango.wrapper import APIWrapper


class Graph(APIWrapper):
    """ArangoDB graph object.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.Connection | arango.batch.Batch
    :param name: the name of the graph
    :type name: str
    """

    _plain_methods = {
        'vertex_collection',
        'edge_collection',
        'name',
        'graph'
    }

    def __init__(self, connection, name):
        self._conn = connection
        self._name = name

    def __repr__(self):
        return "<ArangoDB graph '{}'>".format(self._name)

    @property
    def name(self):
        """Return the name of the graph.

        :returns: the name of the graph
        :rtype: str
        """
        return self._name

    def vertex_collection(self, name):
        """Return the vertex collection."""
        return VertexCollection(self._conn, self._name, name)

    def edge_collection(self, name):
        """Return the edge collection."""
        return EdgeCollection(self._conn, self._name, name)

    def options(self):
        """Return the graph options.

        :returns: the graph options
        :rtype: dict
        :raises: GraphOptionsGetError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise GraphOptionsGetError(res)
            graph = res.body['graph']
            return {
                'id': graph['_id'],
                'name': graph['name'],
                'revision': graph['_rev']
            }
        return request, handler

    ################################
    # Vertex Collection Management #
    ################################

    def orphan_collections(self):
        """Return the orphan (vertex) collections of the graph.

        :returns: the names of the orphan collections
        :rtype: dict
        :raises: GraphOrphanCollectionListError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise OrphanCollectionListError(res)
            return res.body['graph']['orphanCollections']

        return request, handler

    def vertex_collections(self):
        """Return the vertex collections of the graph.

        :returns: the names of the vertex collections
        :rtype: list
        :raises: VertexCollectionListError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexCollectionListError(res)
            return res.body['collections']

        return request, handler

    def create_vertex_collection(self, name):
        """Create a vertex collection for the graph.

        :param name: the name of the vertex collection to create
        :type name: str
        :returns: whether the operation was successful
        :rtype: bool
        :raises: VertexCollectionCreateError
        """
        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex'.format(self._name),
            data={'collection': name}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexCollectionCreateError(res)
            return not res.body['error']

        return request, handler

    def delete_vertex_collection(self, name, purge=False):
        """Remove the vertex collection from the graph.

        :param name: the name of the vertex collection to remove
        :type name: str
        :param purge: drop the vertex collection
        :type purge: bool
        :returns: whether the operation was successful
        :rtype: bool
        :raises: VertexCollectionDeleteError
        """
        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexCollectionDeleteError(res)
            return not res.body['error']

        return request, handler

    ##############################
    # Edge Definition Management #
    ##############################

    def edge_definitions(self):
        """Return the edge definitions of the graph.

        :returns: the edge definitions of the graph
        :rtype: list
        :raises: EdgeDefinitionListError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionListError(res)
            return [
                {
                    'name': edge_definition['collection'],
                    'to_collections': edge_definition['to'],
                    'from_collections': edge_definition['from']
                }
                for edge_definition in
                res.body['graph']['edgeDefinitions']
            ]

        return request, handler

    def create_edge_definition(self, name, from_collections, to_collections):
        """Create a new edge definition for the graph.

        An edge definition consists of an edge collection, ``from`` vertex
        collections and ``to`` vertex collections.

        :param name: the name of the new edge definition
        :type name: str
        :param from_collections: the names of the ``from`` vertex collections
        :type from_collections: list
        :param to_collections: the names of the ``to`` vertex collections
        :type to_collections: list
        :returns: whether the operation was successful
        :rtype: bool
        :raises: EdgeDefinitionCreateError
        """
        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/edge'.format(self._name),
            data={
                'collection': name,
                'from': from_collections,
                'to': to_collections
            }
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionCreateError(res)
            return not res.body['error']

        return request, handler

    def replace_edge_definition(self, name, from_collections, to_collections):
        """Replace the specified edge definition in the graph.

        :param name: the name of the edge definition
        :type name: str
        :param from_collections: the names of the ``from`` vertex collections
        :type from_collections: list
        :param to_collections: the names of the ``to`` vertex collections
        :type to_collections: list
        :returns: whether the operation was successful
        :rtype: bool
        :raises: EdgeDefinitionReplaceError
        """
        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/edge/{}'.format(
                self._name, name
            ),
            data={
                'collection': name,
                'from': from_collections,
                'to': to_collections
            }
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionReplaceError(res)
            return not res.body['error']

        return request, handler

    def delete_edge_definition(self, name, purge=False):
        """Remove the specified edge definition from the graph.

        :param name: the name of the edge definition (collection)
        :type name: str
        :param purge: drop the edge collection as well
        :type purge: bool
        :returns: whether the operation was successful
        :rtype: bool
        :raises: EdgeDefinitionDeleteError
        """
        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/edge/{}'.format(self._name, name),
            params={'dropCollection': purge}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeDefinitionDeleteError(res)
            return not res.body['error']

        return request, handler

    ####################
    # Graph Traversals #
    ####################

    def traverse(self, start, direction=None, strategy=None, order=None,
                 item_order=None, uniqueness=None, max_iterations=None,
                 min_depth=None, max_depth=None, init=None, filters=None,
                 visitor=None, expander=None, sort=None):
        """Execute a graph traversal and return the visited vertices.

        For more details on ``init``, ``filter``, ``visitor``, ``expander``
        and ``sort`` please refer to the ArangoDB HTTP API documentation:
        https://docs.arangodb.com/HttpTraversal/README.html

        :param start: the ID of the start vertex
        :type start: str
        :param direction: "outbound" or "inbound" or "any"
        :type direction: str
        :param strategy: "depthfirst" or "breadthfirst"
        :type strategy: str
        :param order: "preorder" or "postorder"
        :type order: str
        :param item_order: "forward" or "backward"
        :type item_order: str
        :param uniqueness: uniqueness of vertices and edges visited
        :type uniqueness: dict
        :param max_iterations: max number of iterations in each traversal
        :type max_iterations: int
        :param min_depth: minimum traversal depth
        :type min_depth: int
        :param max_depth: maximum traversal depth
        :type max_depth: int
        :param init: custom init function in Javascript
        :type init: str
        :param filters: custom filter function in Javascript
        :type filters: str
        :param visitor: custom visitor function in Javascript
        :type visitor: str
        :param expander: custom expander function in Javascript
        :type expander: str
        :param sort: custom sorting function in Javascript
        :type sort: str
        :returns: the traversal results
        :rtype: dict
        :raises: GraphTraversalError
        """
        data = {
            "startVertex": start,
            "graphName": self._name,
            "direction": direction,
            "strategy": strategy,
            "order": order,
            "itemOrder": item_order,
            "uniqueness": uniqueness,
            "maxIterations": max_iterations,
            "minDepth": min_depth,
            "maxDepth": max_depth,
            "init": init,
            "filter": filters,
            "visitor": visitor,
            "expander": expander,
            "sort": sort
        }
        data = {k: v for k, v in data.items() if v is not None}
        res = self._conn.post("/_api/traversal", data=data)
        if res.status_code not in HTTP_OK:
            raise GraphTraversalError(res)
        return res.body["result"]


class VertexCollection(BaseCollection):

    _plain_methods = {'name', 'graph'}

    def __init__(self, connection, graph, name):
        super(VertexCollection, self).__init__(connection, name, edge=False)
        self._graph = graph

    def __repr__(self):
        return "<ArangoDB vertex collection '{}' in graph '{}'>".format(
            self._name, self._graph
        )

    @property
    def name(self):
        """Return the name of the vertex collection.

        :returns: the name of the vertex collection
        :rtype: str
        """
        return self._name

    @property
    def graph(self):
        """Return the name of the graph.

        :returns: the name of the graph
        :rtype: str
        """
        return self._graph

    def insert(self, data, sync=False):
        """Insert a vertex into the specified vertex collection of the graph.

        If ``data`` contains the ``_key`` field, its value will be used as the
        key of the new vertex. The must be unique in the vertex collection.

        :param data: the body of the new vertex
        :type data: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool
        :returns: the ID, revision and the key of the inserted vertex
        :rtype: dict
        :raises: VertexInsertError
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync
        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex/{}'.format(
                self._graph, self._name
            ),
            data=data,
            params=params
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise VertexInsertError(res)
            return res.body['vertex']

        return request, handler

    def find_by_key(self, key, rev=None):
        """Return the vertex with specified key from the vertex collection.

        If ``revision`` is specified, its value must match against the
        revision of the retrieved vertex.

        :param key: the vertex key
        :type key: str
        :param rev: the vertex revision
        :type rev: str | None
        :returns: the requested vertex or None if not found
        :rtype: dict | None
        :raises: VertexRevisionError, VertexGetError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, key
            ),
            params={'rev': rev} if rev is not None else {}
        )

        def handler(res):
            if res.status_code == 412:
                raise VertexRevisionError(res)
            elif res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise VertexGetError(res)
            return res.body['vertex']

        return request, handler

    def update_by_key(self, key, data, rev=None, sync=False, keep_none=True):
        """Update the specified vertex in the graph.

        If ``keep_none`` is set to True, fields with value None are retained.
        Otherwise, the fields are removed completely from the vertex.

        If ``data`` contains the ``_key`` field, the field is ignored.

        If ``data`` contains the ``_rev`` field, or if ``revision`` is given,
        the revision of the target vertex must match against its value.
        Otherwise, VertexRevisionError is raised.

        :param key: the vertex key
        :type key: str
        :param data: the body to update the vertex with
        :type data: dict
        :param rev: the vertex revision
        :type rev: str | None
        :param keep_none: keep fields with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool
        :returns: the ID, revision and key of the updated vertex
        :rtype: dict
        :raises: VertexRevisionError, VertexUpdateError
        """
        params = {'keepNull': keep_none}
        if sync is not None:
            params['waitForSync'] = sync
        if rev is not None:
            params['rev'] = rev
        elif '_rev' in data:
            params['rev'] = data['_rev']

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, key
            ),
            data=data,
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise VertexRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise VertexUpdateError(res)
            vertex = res.body['vertex']
            vertex['_old_rev'] = vertex.pop('_oldRev')
            return vertex

        return request, handler

    def replace_by_key(self, key, data, rev=None, sync=False):
        """Replace the specified vertex in the graph.

        If ``data`` contains the ``_key`` field, the field is ignored.

        If ``data`` contains the ``_rev`` field, or if ``revision`` is given,
        the revision of the target vertex must match against its value.
        Otherwise, VertexRevisionError is raised.

        :param key: the vertex key
        :type key: str
        :param data: the body to replace the vertex with
        :type data: dict
        :param rev: the vertex revision
        :type rev: str | None
        :param sync: wait for operation to sync to disk
        :type sync: bool
        :returns: the ID, rev and key of the replaced vertex
        :rtype: dict
        :raises: VertexRevisionError, VertexReplaceError
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync
        if rev is not None:
            params['rev'] = rev
        elif '_rev' in data:
            params['rev'] = data['_rev']

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, key
            ),
            params=params,
            data=data
        )

        def handler(res):
            if res.status_code == 412:
                raise VertexRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise VertexReplaceError(res)
            vertex = res.body['vertex']
            vertex['_old_rev'] = vertex.pop('_oldRev')
            return vertex

        return request, handler

    def delete_by_key(self, key, rev=None, sync=False, ignore_missing=True):
        """Delete the vertex of the specified ID from the graph.

        :param key: the vertex key
        :type key: str
        :param rev: the vertex revision must match the value
        :type rev: str | None
        :param sync: wait for the create to sync to disk
        :type sync: bool
        :param ignore_missing: ignore missing vertex
        :type ignore_missing: bool
        :returns: the ID, rev and key of the deleted vertex
        :rtype: dict
        :raises: VertexRevisionError, VertexDeleteError
        """
        params = {"waitForSync": sync}
        if rev is not None:
            params["rev"] = rev

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, key
            ),
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise VertexRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                else:
                    raise VertexDeleteError(res)
            if res.status_code not in HTTP_OK:
                raise VertexDeleteError(res)
            return res.body['removed']

        return request, handler


class EdgeCollection(BaseCollection):

    def __init__(self, connection, graph, name):
        super(EdgeCollection, self).__init__(connection, name, edge=True)
        self._graph = graph

    def __repr__(self):
        return "<ArangoDB edge collection '{}' in graph '{}'>".format(
            self._name, self._graph
        )

    @property
    def name(self):
        """Return the name of the vertex collection.

        :returns: the name of the vertex collection
        :rtype: str
        """
        return self._name

    @property
    def graph(self):
        """Return the name of the graph.

        :returns: the name of the graph
        :rtype: str
        """
        return self._graph

    def insert(self, data, sync=False):
        """Create an edge to the specified edge collection of the graph.

        The ``data`` must contain ``_from`` and ``_to`` keys with valid
        vertex IDs as their values. If ``data`` contains the ``_key`` key,
        its value must be unused in the collection.

        :param data: the body of the new edge
        :type data: dict
        :param sync: wait for the create to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the new edge
        :rtype: dict
        :raises: DocumentInvalidError, EdgeCreateError
        """
        request = Request(
            method='post',
            endpoint="/_api/gharial/{}/edge/{}".format(
                self._graph, self._name
            ),
            data=data,
            params={"waitForSync": sync}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise EdgeCreateError(res)
            return res.body["edge"]

        return request, handler

    def find_by_key(self, key, rev=None):
        """Return the edge of the specified ID in the graph.

        If the edge revision ``rev`` is specified, it must match against
        the revision of the retrieved edge.

        :param key: the key of the edge to retrieve
        :type key: str
        :param rev: the edge revision must match the value
        :type rev: str | None
        :returns: the requested edge | None if not found
        :rtype: dict | None
        :raises: EdgeRevisionError, EdgeGetError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/edge/{}'.format(
                self._graph, self._name, key
            ),
            params={} if rev is None else {"rev": rev}
        )

        def handler(res):
            if res.status_code == 412:
                raise EdgeRevisionError(res)
            elif res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise EdgeGetError(res)
            return res.body["edge"]

        return request, handler

    def update_by_key(self, key, data, rev=None, keep_none=True, sync=False):
        """Update the edge of the specified ID in the graph.

        If ``keep_none`` is set to True, then attributes with values None
        are retained. Otherwise, they are deleted from the edge.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        edge must match against its value. Otherwise a EdgeRevision
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param key: the key of the edge to be deleted
        :type key: str
        :param data: the body to update the edge with
        :type data: dict
        :param rev: the edge revision must match the value
        :type rev: str | None
        :param keep_none: whether or not to keep the keys with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the updated edge
        :rtype: dict
        :raises: EdgeRevisionError, EdgeUpdateError
        """
        params = {
            "waitForSync": sync,
            "keepNull": keep_none
        }
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
            params["rev"] = data["_rev"]

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, key
            ),
            data=data,
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise EdgeRevisionError(res)
            elif res.status_code not in {200, 202}:
                raise EdgeUpdateError(res)
            return res.body["edge"]

        return request, handler

    def replace_by_key(self, key, data, rev=None, sync=False):
        """Replace the edge of the specified ID in the graph.

        If ``data`` contains the ``_key`` key, it is ignored.

        If the ``_rev`` key is in ``data``, the revision of the target
        edge must match against its value. Otherwise a EdgeRevision
        error is thrown. If ``rev`` is also provided, its value is preferred.

        The ``_from`` and ``_to`` attributes are immutable, and they are
        ignored if present in ``data``

        :param key: the key of the edge to be deleted
        :type key: str
        :param data: the body to replace the edge with
        :type data: dict
        :param rev: the edge revision must match the value
        :type rev: str | None
        :param sync: wait for the replace to sync to disk
        :type sync: bool
        :returns: the id, rev and key of the replaced edge
        :rtype: dict
        :raises: EdgeRevisionError, EdgeReplaceError
        """
        params = {"waitForSync": sync}
        if rev is not None:
            params["rev"] = rev
        elif "_rev" in data:
            params["rev"] = data["_rev"]

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, key
            ),
            data=data,
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise EdgeRevisionError(res)
            elif res.status_code not in {200, 202}:
                raise EdgeReplaceError(res)
            return res.body["edge"]

        return request, handler

    def delete_by_key(self, key, rev=None, sync=False, ignore_missing=True):
        """Delete the edge of the specified ID from the graph.

        :param key: the key of the edge to be deleted
        :type key: str
        :param rev: the edge revision must match the value
        :type rev: str | None
        :param sync: wait for the create to sync to disk
        :type sync: bool
        :param ignore_missing: ignore missing vertex
        :type ignore_missing: bool
        :raises: EdgeRevisionError, EdgeDeleteError
        """
        params = {"waitForSync": sync}
        if rev is not None:
            params["rev"] = rev

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, key
            ),
            params=params
        )

        def handler(res):
            if res.status_code == 412:
                raise EdgeRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                else:
                    raise EdgeDeleteError(res)
            elif res.status_code not in HTTP_OK:
                raise EdgeDeleteError(res)
            return not res.body['error']

        return request, handler
