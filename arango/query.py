from __future__ import absolute_import, unicode_literals

from arango.constants import HTTP_OK
from arango.cursor import Cursor
from arango.exceptions import *


class Query(object):
    """ArangoDB AQL query object.

    :param connection: ArangoDB API connection object
    :type connection: arango.connection.Connection
    """

    def __init__(self, connection):
        self._conn = connection
        self._cache = QueryCache(self._conn)

    def __repr__(self):
        return "<ArangoDB Query>"

    @property
    def cache(self):
        return self._cache

    def explain(self, query, all_plans=False, max_plans=None,
                optimizer_rules=None):
        """Explain the AQL query.

        This method does not execute the query, but only inspects it and
        returns the metadata.

        If ``all_plans`` is set to True, all possible execution plans are
        returned. Otherwise only the optimal plan is returned.

        For more information on ``optimizer_rules``, refer to:
        https://docs.arangodb.com/HttpAqlQuery/README.html

        :param query: the AQL query to explain
        :type query: str
        :param all_plans: whether or not to return all execution plans
        :type all_plans: bool
        :param max_plans: maximum number of plans the optimizer generates
        :type max_plans: None or int
        :param optimizer_rules: list of optimizer rules
        :type optimizer_rules: list
        :returns: the query plan or list of plans (if all_plans is True)
        :rtype: dict or list
        :raises: QueryExplainError
        """
        options = {'allPlans': all_plans}
        if max_plans is not None:
            options['maxNumberOfPlans'] = max_plans
        if optimizer_rules is not None:
            options['optimizer'] = {'rules': optimizer_rules}
        res = self._conn.post(
            '/_api/explain', data={'query': query, 'options': options}
        )
        if res.status_code not in HTTP_OK:
            raise QueryExplainError(res)
        if 'plan' in res.body:
            return res.body['plan']
        else:
            return res.body['plans']

    def validate(self, query):
        """Validate the AQL query.

        :param query: the AQL query to validate
        :type query: str
        :returns: whether the validation was successful
        :rtype: bool
        :raises: QueryValidateError
        """
        res = self._conn.post('/_api/query', data={'query': query})
        if res.status_code not in HTTP_OK:
            raise QueryValidateError(res)
        res.body.pop('code', None)
        res.body.pop('error', None)
        return res.body

    def execute(self, query, count=False, batch_size=None, ttl=None,
                bind_vars=None, full_count=None, max_plans=None,
                optimizer_rules=None):
        """Execute the AQL query and return the result.

        For more information on ``full_count`` please refer to:
        https://docs.arangodb.com/HttpAqlQueryCursor/AccessingCursors.html

        :param query: the AQL query to execute
        :type query: str
        :param count: whether or not the document count should be returned
        :type count: bool
        :param batch_size: maximum number of documents in one round trip
        :type batch_size: int
        :param ttl: time-to-live for the cursor (in seconds)
        :type ttl: int
        :param bind_vars: key-value pairs of bind parameters
        :type bind_vars: dict
        :param full_count: whether or not to include count before last LIMIT
        :param max_plans: maximum number of plans the optimizer generates
        :type max_plans: None or int
        :param optimizer_rules: list of optimizer rules
        :type optimizer_rules: list
        :returns: document cursor
        :rtype: arango.cursor.Cursor
        :raises: AQLQueryExecuteError, CursorCloseError
        """
        options = {}
        if full_count is not None:
            options['fullCount'] = full_count
        if max_plans is not None:
            options['maxNumberOfPlans'] = max_plans
        if optimizer_rules is not None:
            options['optimizer'] = {'rules': optimizer_rules}

        data = {'query': query, 'count': count}
        if batch_size is not None:
            data['batchSize'] = batch_size
        if ttl is not None:
            data['ttl'] = ttl
        if bind_vars is not None:
            data['bindVars'] = bind_vars
        if options:
            data['options'] = options

        res = self._conn.post('/_api/cursor', data=data)
        if res.status_code not in HTTP_OK:
            raise AQLQueryExecuteError(res)
        return Cursor(self._conn, res.body)

    def functions(self):
        """List the AQL functions defined in this database.

        :returns: a mapping of AQL function names to its javascript code
        :rtype: dict
        :raises: AQLFunctionListError
        """
        res = self._conn.get('/_api/aqlfunction')
        if res.status_code not in HTTP_OK:
            raise AQLFunctionListError(res)
        body = res.body or {}
        return {func['name']: func['code'] for func in body}

    def create_function(self, name, code):
        """Create a new AQL function.

        :param name: the name of the new AQL function to create
        :type name: str
        :param code: the stringified javascript code of the new function
        :type code: str
        :returns: whether the AQL function was created successfully
        :rtype: bool
        :raises: AQLFunctionCreateError
        """
        data = {'name': name, 'code': code}
        res = self._conn.post('/_api/aqlfunction', data=data)
        if res.status_code not in (200, 201):
            raise AQLFunctionCreateError(res)
        return not res.body['error']

    def delete_function(self, name, group=None, ignore_missing=False):
        """Delete the AQL function of the given name.

        If ``group`` is set to True, then the function name provided in
        ``name`` is treated as a namespace prefix, and all functions in
        the specified namespace will be deleted. If set to False, the
        function name provided in ``name`` must be fully qualified,
        including any namespaces.

        :param name: the name of the AQL function to delete
        :type name: str
        :param group: whether to treat the name as a namespace prefix
        :type group: bool
        :param ignore_missing: whether to ignore 404
        :type ignore_missing: bool
        :returns: whether the AQL function was deleted successfully
        :rtype: bool
        :raises: AQLFunctionDeleteError
        """
        res = self._conn.delete(
            '/_api/aqlfunction/{}'.format(name),
            params={'group': group} if group is not None else {}
        )
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise AQLFunctionDeleteError(res)
        return not res.body['error']


class QueryCache(object):

    def __init__(self, connection):
        """Initialize the AQL wrapper object.

        :param connection: ArangoDB API connection object
        :type connection: arango.connection.Connection
        """
        self._conn = connection

    def properties(self):
        """Return the configuration of the AQL query cache.

        :returns: the result
        :rtype: dict
        :raises: AQLQueryCacheGetError
        """
        res = self._conn.get('/_api/query-cache/properties')
        if res.status_code not in HTTP_OK:
            raise AQLQueryCacheGetError(res)
        return {
            'mode': res.body['mode'],
            'limit': res.body['maxResults']
        }

    def set_properties(self, mode=None, limit=None):
        """Configure the AQL query cache.

        :param mode: the mode to operate in (off/on/demand)
        :type mode: str | None
        :param limit: max number of results to be stored
        :type limit: int | None
        :returns: the response
        :rtype: dict
        :raises: AQLQueryCacheSetError
        """
        data = {}
        if mode is not None:
            data['mode'] = mode
        if limit is not None:
            data['maxResults'] = limit
        res = self._conn.put(
            '/_api/query-cache/properties',
            data=data
        )
        if res.status_code not in HTTP_OK:
            raise AQLQueryCacheConfigureError(res)
        return {
            'mode': res.body['mode'],
            'limit': res.body['maxResults']
        }

    def clear(self):
        """Clear any results in the AQL query cache.

        :returns: the result
        :rtype: dict
        :raises: AQLQueryCacheDeleteError
        """
        res = self._conn.delete('/_api/query-cache')
        if res.status_code not in HTTP_OK:
            raise AQLQueryCacheClearError(res)
        return not res.body['error']