from __future__ import unicode_literals

from arango.exceptions import (
    ArangoError,
    JobInvalidError,
    JobNotFoundError
)


class Job(object):

    def __init__(self, connection, job_id, handler=lambda res: res.body):
        self._conn = connection
        self._id = job_id
        self._handler = handler

    @property
    def id(self):
        return self._id

    def result(self):
        """Get the result of the job from the server."""
        res = self._conn.find_by_key('/_api/job/{}'.format(self._id))
        if res.status_code == 200:
            print(res.text)
            print(res.body)
            return self._handler(res)
        elif res.status_code == 400:
            raise JobInvalidError(res)
        elif res.status_code == 404:
            raise JobNotFoundError(res)
        else:
            raise ArangoError(res)

    def pop(self):
        """Pop the result of the job from the server."""
        res = self._conn.put('/_api/job/{}'.format(self._id))
        if res.status_code == 200:
            return self._handler(res) if self._handler else res.body
        elif res.status_code == 204:
            return None
        elif res.status_code == 400:
            raise JobInvalidError(res)
        elif res.status_code == 404:
            raise JobNotFoundError(res)
        else:
            raise ArangoError(res)

    def delete(self):
        """Delete the result of the job from the server."""
        res = self._conn.delete_by_key('/_api/job/{}'.format(self._id))
        if res.status_code == 200:
            return True
        elif res.status_code == 400:
            raise JobInvalidError(res)
        elif res.status_code == 404:
            raise JobNotFoundError(res)
        else:
            raise ArangoError(res)

    def cancel(self):
        """Cancel the currently running job."""
        res = self._conn.put('/_api/job/{}/cancel'.format(self.id))
        if res.status_code == 200:
            return True
        elif res.status_code == 400:
            raise JobInvalidError(res)
        elif res.status_code == 404:
            raise JobNotFoundError(res)
        else:
            raise ArangoError(res)
