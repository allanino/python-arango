from __future__ import unicode_literals

from arango.exceptions import *


class Transaction(object):
    """ArangoDB transaction object.

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    """

    def __init__(self, connection):
        self._conn = connection



    # def execute_transaction(self, action, read_collections=None,
    #                         write_collections=None, params=None,
    #                         sync=False, lock_timeout=None):
    #     """Execute the transaction and return the result.
    #
    #     Setting the ``lock_timeout`` to 0 will make ArangoDB not time out
    #     waiting for a lock.
    #
    #     :param action: the javascript commands to be executed
    #     :type action: str
    #     :param read_collections: the collections read
    #     :type read_collections: str or list | None
    #     :param write_collections: the collections written to
    #     :type write_collections: str or list | None
    #     :param params: Parameters for the function in action
    #     :type params: list or dict | None
    #     :param sync: wait for the transaction to sync to disk
    #     :type sync: bool
    #     :param lock_timeout: timeout for waiting on collection locks
    #     :type lock_timeout: int | None
    #     :returns: the results of the execution
    #     :rtype: dict
    #     :raises: TransactionExecuteError
    #     """
    #     path = '/_api/transaction'
    #     data = {'collections': {}, 'action': action}
    #     if read_collections is not None:
    #         data['collections']['read'] = read_collections
    #     if write_collections is not None:
    #         data['collections']['write'] = write_collections
    #     if params is not None:
    #         data['params'] = params
    #     http_params = {
    #         'waitForSync': sync,
    #         'lockTimeout': lock_timeout,
    #     }
    #     res = self._conn.post(endpoint=path, data=data, params=http_params)
    #     if res.status_code not in HTTP_OK:
    #         raise TransactionExecuteError(res)
    #     return res.body['result']
