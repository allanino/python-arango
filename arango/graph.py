from __future__ import absolute_import, unicode_literals

from arango.collection import VertexCollection, EdgeCollection
from arango.constants import HTTP_OK
from arango.exceptions import *
from arango.request import Request
from arango.api import APIWrapper


class Graph(APIWrapper):
    """ArangoDB graph object.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.Connection
    :param name: the name of the graph
    :type name: str
    """

    _normal_methods = {
        'vertex_collection', 'v',
        'edge_collection', 'e',
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

    def v(self, name):
        """Alias for self.vertex_collection."""
        return self.vertex_collection(name)

    def e(self, name):
        """Alias for self.edge_collection."""
        return self.edge_collection(name)

    def vertex_collection(self, name):
        """Return the vertex collection."""
        return VertexCollection(self._conn, self._name, name)

    def edge_collection(self, name):
        """Return the edge collection."""
        return EdgeCollection(self._conn, self._name, name)

    def properties(self):
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

    def edge_collections(self):
        """Return the edge collections/definitions of the graph.

        :returns: the edge collections/definitions of the graph
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

    def create_edge_collection(self, name, from_collections, to_collections):
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

    def replace_edge_collection(self, name, from_collections, to_collections):
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

    def delete_edge_collection(self, name, purge=False):
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
