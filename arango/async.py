from __future__ import absolute_import, unicode_literals

from arango.constants import HTTP_OK
from arango.connection import Connection
from arango.collection import Collection
from arango.exceptions import (
    AsyncExecuteError,
    AsyncJobInvalidError,
    AsyncJobNotFoundError,
    AsyncJobCancelError,
    AsyncResultGetError,
    AsyncResultPopError,
    AsyncResultDeleteError
)
from arango.graph import Graph


class AsyncExecution(Connection):
    """ArangoDB asynchronous execution object."""

    def __init__(self, connection, return_result=True):
        super(AsyncExecution, self).__init__(
            protocol=connection.protocol,
            host=connection.host,
            port=connection.port,
            username=connection.username,
            password=connection.password,
            client=connection.client,
            database=connection.database
        )
        self._return_result = return_result

    def __repr__(self):
        return '<ArangoDB asynchronous execution>'

    def handle_request(self, request, handler):
        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = True

        res = getattr(self, request.method)(**request.kwargs)
        if res.status_code not in HTTP_OK:
            raise AsyncExecuteError(res)
        if self._return_result:
            return AsyncJob(self, res.headers['x-arango-async-id'], handler)

    def collection(self, name):
        """Return the Collection object of the specified name.

        :param name: the name of the collection
        :type name: str
        :param edge: whether this collection is an edge collection
        :type edge: bool
        :returns: the requested collection object
        :rtype: arango.collection.Collection
        :raises: TypeError
        """
        return Collection(self, name)

    def graph(self, name):
        """Return the Graph object of the specified name.

        :param name: the name of the graph
        :type name: str
        :returns: the requested graph object
        :rtype: arango.graph.Graph
        :raises: TypeError, GraphNotFound
        """
        return Graph(self, name)


class AsyncJob(object):

    def __init__(self, connection, job_id, handler):
        self._conn = connection
        self._id = job_id
        self._handler = handler

    def __repr__(self):
        return '<ArangoDB asynchronous job {}>'.format(self._id)

    @property
    def id(self):
        """Return the job ID.

        :returns: the job ID
        :rtype: str | None
        """
        return self._id

    def result(self):
        """Get the result of the job from the server."""
        res = self._conn.get('/_api/job/{}'.format(self._id))
        if res.status_code == 200:
            return self._handler(res)
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncResultGetError(res)

    def pop(self):
        """Pop the result of the job from the server."""
        res = self._conn.put('/_api/job/{}'.format(self._id))
        if res.status_code == 200:
            return self._handler(res) if self._handler else res.body
        elif res.status_code == 204:
            return None
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncResultPopError(res)

    def delete(self):
        """Delete the result of the job from the server."""
        res = self._conn.delete('/_api/job/{}'.format(self._id))
        if res.status_code == 200:
            return True
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncResultDeleteError(res)

    def cancel(self):
        """Cancel the currently running job."""
        res = self._conn.put('/_api/job/{}/cancel'.format(self.id))
        if res.status_code == 200:
            return True
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncJobCancelError(res)
