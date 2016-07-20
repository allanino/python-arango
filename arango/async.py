from __future__ import absolute_import, unicode_literals

from arango.collection import Collection
from arango.connection import Connection
from arango.constants import HTTP_OK
from arango.exceptions import (
    AsyncExecuteError,
    AsyncJobInvalidError,
    AsyncJobNotDoneError,
    AsyncJobNotFoundError,
    AsyncJobCancelError,
    AsyncJobStatusError,
    AsyncJobResultGetError,
    AsyncJobResultDeleteError
)
from arango.graph import Graph
from arango.query import Query


class AsyncExecution(Connection):
    """ArangoDB asynchronous execution object.

    API requests via AsyncExecution are placed in a server-side in-memory task
    queue and executed asynchronously in a fire-and-forget style.

    If ``return_result`` is set to True, an AsyncJob instance is returned each
    time a request is issued. The AsyncJob object can be used to keep track of
    the status of the request and retrieve the result.

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    :param return_result: whether to store and return the result
    :type return_result: bool
    """

    def __init__(self, connection, return_result=True):
        super(AsyncExecution, self).__init__(
            protocol=connection.protocol,
            host=connection.host,
            port=connection.port,
            username=connection.username,
            password=connection.password,
            http_client=connection.client,
            database=connection.database
        )
        self._return_result = return_result
        self._query = Query(self)

    def __repr__(self):
        return '<ArangoDB async execution object>'

    def handle_request(self, request, handler):
        """Handle the incoming request and response handler objects.

        This method designed to be used internally only.

        The ``request`` and its corresponding ``handler`` are placed in a
        server-side in-memory task queue and executed asynchronously.

        If ``return_response`` was set to True during the initialization of
        the AsyncExecution object, an AsyncJob instance is returned.

        :param request: ArangoDB request object
        :type request: arango.request.Request
        :param handler: ArangoDB response handler
        :type handler: callable
        :return: ArangoDB asynchronous job object or None
        :rtype: arango.async.AsyncJob | None
        """
        if self._return_result:
            request.headers['x-arango-async'] = 'store'
        else:
            request.headers['x-arango-async'] = True

        res = getattr(self, request.method)(**request.kwargs)
        if res.status_code not in HTTP_OK:
            raise AsyncExecuteError(res)
        if self._return_result:
            return AsyncJob(self, res.headers['x-arango-async-id'], handler)

    @property
    def query(self):
        """Return the query object for asynchronous execution.

        API requests via the returned query object are placed in a server-side
        in-memory task queue and executed asynchronously in a fire-and-forget
        style.

        :returns: ArangoDB query object
        :rtype: arango.query.Query
        """
        return self._query

    def collection(self, name):
        """Return a collection object for asynchronous execution.

        API requests via the returned collection object are placed in a
        server-side in-memory task queue and executed asynchronously in
        a fire-and-forget style.

        :param name: the name of the collection
        :type name: str
        :returns: the collection object
        :rtype: arango.collection.Collection
        """
        return Collection(self, name)

    def graph(self, name):
        """Return a graph object for asynchronous execution.

        API requests via the returned graph object are placed in a server-side
        in-memory task queue and executed asynchronously in a fire-and-forget
        style.

        :param name: the name of the graph
        :type name: str
        :returns: the graph object
        :rtype: arango.graph.Graph
        """
        return Graph(self, name)

    def c(self, name):
        """Alias for self.collection."""
        return self.collection(name)

    def g(self, name):
        """Alias for self.graph."""
        return self.graph(name)


class AsyncJob(object):
    """ArangoDB asynchronous job object.

    The AsyncJob object is used to keep track of the status of a particular
    API request and retrieve the result or error when available.

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    :param job_id: the ID of the asynchronous job
    :type job_id: str | None
    :param handler: ArangoDB response handler
    :type handler: callable
    """

    class Status:
        """Asynchronous jobs statuses.

        The status can be one of the following:

        PENDING:  the job is pending in the queue
        DONE:     the job completed successfully
        ERROR:    the job raised an exception
        """
        PENDING = 'pending'
        DONE = 'done'
        ERROR = 'error'

    def __init__(self, connection, job_id, handler):
        self._conn = connection
        self._id = job_id
        self._handler = handler

    def __repr__(self):
        return '<ArangoDB asynchronous job {}>'.format(self._id)

    @property
    def id(self):
        """Return the ID of the job.

        :returns: the ID of the job
        :rtype: str | None
        """
        return self._id

    def status(self):
        """Return the status of the asynchronous job from the server.

        The status can be one of the following:

        BatchJob.Status.PENDING: the job is still pending in the queue
        BatchJob.Status.DONE:    the job completed successfully
        BatchJob.Status.ERROR:   the job raised an exception

        :returns: the result of the asynchronous job
        :rtype: int
        :raises: AsyncJobInvalidError,
                 AsyncJobNotFoundError,
                 AsyncJobStatusError
        """
        res = self._conn.get('/_api/job/{}'.format(self._id))
        if res.status_code == 204:
            return self.Status.PENDING
        elif res.status_code in HTTP_OK:
            return self.Status.DONE
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncJobStatusError(res)

    def cancel(self, ignore_missing=False):
        """Cancel the asynchronous job.

        The job can be cancelled only when it is still pending in the queue.

        If the job is cancelled successfully, boolean True is returned. If
        the job was not found but ``ignore_missing`` was set, boolean False
        is returned.

        :param ignore_missing: do not raise an exception if the job is missing
        :type ignore_missing: bool
        :returns: whether the operation was successful
        :rtype: bool
        :raises: AsyncJobInvalidError,
                 AsyncJobNotFoundError,
                 AsyncJobResultGetError
        """
        res = self._conn.put('/_api/job/{}/cancel'.format(self._id))
        if res.status_code == 200:
            return True
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            if ignore_missing:
                return False
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncJobCancelError(res)

    def result(self):
        """Return the result of the job, if available.

        If the job is still pending, AsyncJobNotDoneError is raised.

        :returns: the result of the asynchronous job
        :rtype: object
        :raises: AsyncJobInvalidError,
                 AsyncJobNotDoneError,
                 AsyncJobNotFoundError,
                 AsyncJobResultGetError
        """
        res = self._conn.put('/_api/job/{}'.format(self._id))
        if res.status_code == 204:
            return AsyncJobNotDoneError(self._id)
        elif res.status_code in HTTP_OK:
            return self._handler(res)
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            # TODO: figure out a more elegant way to differentiate b/w 404s
            print(res.body)
            if (
                res.body is not None and
                res.body.get('errorNum') == 404 and
                res.body.get('errorMessage') == 'not found'
            ):
                raise AsyncJobNotFoundError(res)
            return self._handler(res)
        else:
            raise AsyncJobResultGetError(res)

    def delete(self, ignore_missing=False):
        """Clear the result of the job from the server if any.

        If the result is deleted successfully, boolean True is returned. If
        the job was not found but ``ignore_missing`` was set, boolean False
        is returned.

        :param ignore_missing: do not raise an exception if the job is missing
        :type ignore_missing: bool
        :returns: whether the operation was successful
        :rtype: bool
        :raises: AsyncJobInvalidError,
                 AsyncJobNotFoundError,
                 AsyncJobResultDeleteError
        """
        res = self._conn.delete('/_api/job/{}'.format(self._id))
        if res.status_code in HTTP_OK:
            return True
        elif res.status_code == 400:
            raise AsyncJobInvalidError(res)
        elif res.status_code == 404:
            if ignore_missing:
                return False
            raise AsyncJobNotFoundError(res)
        else:
            raise AsyncJobResultDeleteError(res)
