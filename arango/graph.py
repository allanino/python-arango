from __future__ import absolute_import, unicode_literals

from arango.collection import VertexCollection, EdgeCollection
from arango.constants import HTTP_OK
from arango.exceptions import *
from arango.request import Request
from arango.wrapper import APIWrapper


class Graph(APIWrapper):
    """ArangoDB graph object.

    A graph can have vertex and edge collections.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.Connection
    :param name: the name of the graph
    :type name: str
    """

    _standard_methods = {
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
        """Return the graph properties.

        :returns: the graph properties
        :rtype: dict
        :raises: GraphGetPropertiesError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise GraphGetPropertiesError(res)
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
        """Return the orphan vertex collections of the graph.

        :returns: the orphan vertex collections
        :rtype: dict
        :raises:
        OrphanCollectionListError
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
        :returns: the vertex collection object
        :rtype: arango.collection.VertexCollection
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
            return VertexCollection(self._conn, self._name, name)

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
        :returns: the edge collection object
        :rtype: arango.collection.EdgeCollection
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
            return EdgeCollection(self._conn, self._name, name)

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

    def traverse(self,
                 start_vertex,
                 direction=None,
                 strategy=None,
                 order=None,
                 item_order=None,
                 edge_uniqueness=None,
                 vertex_uniqueness=None,
                 max_iter=None,
                 min_depth=None,
                 max_depth=None,
                 init_func=None,
                 sort_func=None,
                 filter_func=None,
                 visitor_func=None,
                 expander_func=None):
        """Traverse the graph and return the visited vertices and edges.

        ``init_func`` is a Java

         must be a JavaScript function (str) with signature:
        (config, result) -> void, and is used to initialize any values in result argument

        ``sort_func`` must be a JavaScript function with signature:
        (left, right) -> integer, and must return -1 if left is smaller than
        right, +1 if left is greater than right, and 0 if left and right are
        equal.

        and must return


        :param start_vertex: the ID of the start vertex
        :type start_vertex: str
        :param direction: "outbound", "inbound" or "any" (default)
        :type direction: str
        :param strategy: "dfs" or "bfs"
        :type strategy: str
        :param order: "preorder", "postorder" or "preorder-expander"
        :type order: str
        :param item_order: "forward" or "backward"
        :type item_order: str
        :param vertex_uniqueness: "none", "global" or "path"
        :type vertex_uniqueness: str
        :param edge_uniqueness: "none", "global" or "path"
        :type edge_uniqueness: str
        :param max_iter: halt the graph traversal aborts after the max number
            of iterations (set this flag to prevent endless loops in cyclic
            graphs)
        :type max_iter: int
        :param min_depth: the minimum depth of the nodes to visit
        :type min_depth: int
        :param max_depth: the maximum depth of the nodes to visit
        :type max_depth: int
        :param init_func: custom initialize function (in JavaScript) with
        :type init_func: str
        :param sort_func: custom sort function (in JavaScript)
        :type sort_func: str
        :param filter_func: custom filter function (in JavaScript)
        :type filter_func: str
        :param visitor_func: custom visitor function (in JavaScript)
        :type visitor_func: str
        :param expander_func: customer expander function (in JavaScript)
        :type expander_func: str

        :returns: the traversal results
        :rtype: dict
        :raises: GraphTraverseError
        """
        if expander_func is None and direction is None:
            direction = 'any'

        if strategy is not None:
            if strategy.lower() == 'dfs':
                strategy = 'depthfirst'
            elif strategy.lower() == 'bfs':
                strategy = 'breadthfirst'

        uniqueness = {}
        if vertex_uniqueness is not None:
            uniqueness['vertices'] = vertex_uniqueness
        if edge_uniqueness is not None:
            uniqueness['edges'] = edge_uniqueness

        data = {
            'startVertex': start_vertex,
            'graphName': self._name,
            'direction': direction,
            'strategy': strategy,
            'order': order,
            'itemOrder': item_order,
            'uniqueness': uniqueness or None,
            'maxIterations': max_iter,
            'minDepth': min_depth,
            'maxDepth': max_depth,
            'init': init_func,
            'filter': filter_func,
            'visitor': visitor_func,
            'sort': sort_func
        }
        data = {k: v for k, v in data.items() if v is not None}

        request = Request(
            method='post',
            endpoint='/_api/traversal',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise GraphTraverseError(res)
            return res.body['result']

        return request, handler
