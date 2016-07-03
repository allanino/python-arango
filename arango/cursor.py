from __future__ import absolute_import, unicode_literals

from arango.constants import HTTP_OK
from arango.exceptions import (
    CursorNextError,
    CursorCloseError,
)


class Cursor(object):
    """ArangoDB cursor object.

    Fetches documents from the server cursor in batches. The ``init_data`` is
    the response body of the create cursor API call.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.Connection
    :param init_data: the cursor initialization data
    :type init_data: dict
    :raises: CursorNextError, CursorCloseError
    """

    def __init__(self, connection, init_data):
        self._conn = connection
        self._data = init_data

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    @property
    def id(self):
        """Return the cursor ID.

        :returns: the cursor ID
        :rtype: str | None
        """
        return self._data.get('id')

    @property
    def current_batch(self):
        """Return the current batch.

        :returns: the current batch
        :rtype: list
        """
        return self._data['result']

    @property
    def has_more(self):
        """Indicates whether more results are available.

        :returns: whether more results are available
        :rtype: bool
        """
        return self._data['hasMore']

    @property
    def count(self):
        """Return the total number of results.

        If the cursor was not initialized with the count option enabled,
        None is returned instead.

        :return: the total number of results
        :rtype: int
        """
        return self._data.get('count')

    @property
    def cached(self):
        return self._data.get('cached')

    @property
    def extra(self):
        return self._data.get('extra')

    def next(self):
        """Read the next result from the cursor.

        :returns: the next item in the cursor
        :rtype: dict
        :raises: StopIteration, CursorNextError
        """
        if not self.current_batch and self.has_more:
            res = self._conn.put("/_api/cursor/{}".format(self.id))
            if res.status_code not in HTTP_OK:
                raise CursorNextError(res)
            self._data = res.body
        elif not self.current_batch and not self.has_more:
            self.close()
            raise StopIteration
        return self.current_batch.pop(0)

    def close(self):
        """Close the cursor and free the resources tied to it.

        :returns: whether the cursor was closed successfully
        :rtype: bool
        :raises: CursorCloseError
        """
        if not self.id:
            return False
        res = self._conn.delete("/api/cursor/{}".format(self.id))
        if res.status_code not in {404, 202}:
            raise CursorCloseError(res)
        return True

