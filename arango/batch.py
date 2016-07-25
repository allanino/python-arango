from __future__ import absolute_import, unicode_literals

from arango.collection import Collection
from arango.connection import Connection
from arango.constants import HTTP_OK
from arango.exceptions import BatchExecuteError
from arango.graph import Graph
from arango.response import Response
from arango.query import Query


class BatchRequest(Connection):
    """ArangoDB batch request object.

    API requests via this class are placed in an in-memory queue committed as
    a whole in a single HTTP call to the ArangoDB server.

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    :param return_result: If set to `True`, a :class:`arango.batch.BatchJob`
        instance is returned each time a request is issued. The instance can
        be used to retrieve the result of the corresponding request after the
        commit.
    :type return_result: bool
    """

    def __init__(self, connection, return_result=True):
        super(BatchRequest, self).__init__(
            protocol=connection.protocol,
            host=connection.host,
            port=connection.port,
            username=connection.username,
            password=connection.password,
            http_client=connection.client,
            database=connection.database
        )
        self._return_result = return_result
        self._requests = []    # The queue for requests
        self._handlers = []    # The queue for response handlers
        self._batch_jobs = []  # For tracking batch jobs
        self._query = Query(self)

    def __repr__(self):
        return '<ArangoDB batch request object>'

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return self.commit()

    def handle_request(self, request, handler):
        """Handle the incoming request and response handler objects.

        :param request: ArangoDB request object queued as part of the current
            batch request scope, and run only when the batch is committed
            via :func:`~arango.Batch.commit`
        :type request: arango.request.Request
        :param handler: ArangoDB response handler
        :type handler: callable
        :returns: the batch job or None
        :rtype: arango.batch.BatchJob | None

        .. Note::
            This method designed to be used internally only.
        """
        self._requests.append(request)
        self._handlers.append(handler)

        if self._return_result:
            batch_job = BatchJob()
            self._batch_jobs.append(batch_job)
            return batch_job

    def commit(self):
        """Execute the API requests in a single HTTP call.

        If ``return_response`` was set to True during the initialization of
        the BatchRequest object, the responses are returned and also saved
        within the object for later retrieval (via the BatchRequest.result
        method). The responses are ordered with respect to the order the
        requests were originally queued in the BatchRequest instance.

        If ``return_response`` was set to False, an empty result set will be
        returned instead and the responses are not saved.

        :raises: BatchExecuteError
        """
        try:
            if not self._requests:
                return
            raw_data = ''
            for content_id, request in enumerate(self._requests, start=1):
                raw_data += '--XXXsubpartXXX\r\n'
                raw_data += 'Content-Type: application/x-arango-batchpart\r\n'
                raw_data += 'Content-Id: {}\r\n\r\n'.format(content_id)
                raw_data += '{}\r\n'.format(request.stringify())
            raw_data += '--XXXsubpartXXX--\r\n\r\n'

            res = self.post(
                endpoint='/_api/batch',
                headers={
                    'Content-Type': (
                        'multipart/form-data; boundary=XXXsubpartXXX'
                    )
                },
                data=raw_data,
            )
            if res.status_code not in HTTP_OK:
                raise BatchExecuteError(res)
            if not self._return_result:
                return

            for index, raw_response in enumerate(
                res.raw_body.split('--XXXsubpartXXX')[1:-1]
            ):
                request = self._requests[index]
                handler = self._handlers[index]
                job = self._batch_jobs[index]
                res_parts = raw_response.strip().split('\r\n')
                raw_status, raw_body = res_parts[3], res_parts[-1]
                _, status_code, status_text = raw_status.split(' ', 2)
                try:
                    result = handler(Response(
                        method=request.method,
                        url=self._url_prefix + request.endpoint,
                        headers=request.headers,
                        status_code=int(status_code),
                        status_text=status_text,
                        body=raw_body
                    ))
                except Exception as err:
                    job.update(status=BatchJob.ERROR, exception=err)
                else:
                    job.update(status=BatchJob.DONE, result=result)
        finally:
            self._requests, self._handlers, self._batch_jobs = [], [], []

    @property
    def query(self):
        """Return the query object for the batch request.

        API requests via the returned query object are placed in an in-memory
        queue inside BatchRequest object and committed as a whole in a single
        HTTP call to the ArangoDB server.

        :returns: ArangoDB query object
        :rtype: arango.query.Query
        """
        return self._query

    def collection(self, name):
        """Return a collection object for the batch request.

        API requests via the returned collection object are placed in an
        in-memory queue inside BatchRequest object and committed as a
        whole in a single HTTP call to the ArangoDB server.

        :param name: the name of the collection
        :type name: str
        :returns: the collection object
        :rtype: arango.collection.Collection
        """
        return Collection(self, name)

    def graph(self, name):
        """Return a graph object for the batch request.

        API requests via the returned graph object are placed in an in-memory
        queue inside BatchRequest object and committed as a whole in a single
        HTTP call to the ArangoDB server.

        :param name: the name of the graph
        :type name: str
        :returns: the graph object
        :rtype: arango.graph.Graph
        """
        return Graph(self, name)

    def c(self, name):
        """Alias for method :func:`~arango.batch.BatchRequest.collection`."""
        return self.collection(name)

    def g(self, name):
        """Alias for method :func:`~arango.batch.BatchRequest.graph`."""
        return self.graph(name)


class BatchJob(object):
    """ArangoDB batch job object which holds information of a batch request.

    The BatchJob object is used to keep track of the status of a particular
    API request and retrieve the result or error when available.

    When the batch request is committed, the BatchJob instance is updated
    automatically with the result or the error from the particular request.
    """

    PENDING = 'pending'
    DONE = 'done'
    ERROR = 'error'

    def __init__(self):
        self._status = self.PENDING
        self._result = None
        self._exception = None

    def update(self, status, result=None, exception=None):
        """Update the status, result and the exception of the batch job.

        This method designed to be used internally only.

        :param status: the status of the batch job
        :type status: int
        :param result: the result of the batch job, if any.
        :type result: object
        :param exception: the exception raised by the batch job, if any.
        :type exception: Exception
        """
        self._status = status
        self._result = result
        self._exception = exception

    @property
    def status(self):
        """Return the status of the job.

        AsyncJob.PENDING: the job is still waiting to be committed
        AsyncJob.DONE:    the job completed successfully
        AsyncJob.ERROR:   the job raised an exception

        :returns: the batch job status
        :rtype: int
        """
        return self._status

    def exception(self):
        """Return the exception raised by the job, if any.

        If there were no exceptions raised, None is returned

        :returns: the exception raised by the batch job
        :rtype: Exception | None
        """
        return self._exception

    def result(self):
        """Return the result of the job, if available.

        If the job failed, None is returned. Since it is not possible to tell
        whether the job failed or the result is actually None, it is advisable
        to use BatchJob.status to check first.

        :returns: the result of the batch job
        :rtype: object
        """
        return self._result
