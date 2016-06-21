from __future__ import absolute_import, unicode_literals

from json import dumps
from datetime import datetime

from six import string_types as string
from requests import ConnectionError

from arango.clients import DefaultHTTPClient
from arango.constants import HTTP_OK
from arango.database import Database
from arango.exceptions import *
from arango.wal import WriteAheadLog


class Connection(object):
    """ArangoDB database connection.

    :param protocol: the internet transfer protocol (default: 'http')
    :type protocol: str
    :param host: ArangoDB host (default: 'localhost')
    :type host: str
    :param port: ArangoDB port (default: 8529)
    :type port: int or str
    :param username: ArangoDB username (default: 'root')
    :type username: str
    :param password: ArangoDB password (default: '')
    :type password: str
    :param verify: check the connection during initialization
    :type verify: bool
    :param client: the HTTP client
    :type client: arango.clients.base.BaseHTTPClient | None
    """

    def __init__(self, protocol='http', host='localhost', port=9000,
                 username='root', password='', verify=True, client=None):
        self.protocol = protocol
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.verify = verify
        self.client = client or DefaultHTTPClient()

        self.wal = WriteAheadLog(self)
        self._type = 'normal'
        self._registered_db = '_system'

        # Verify the server connection
        if self.verify:
            res = self.head('/_api/version')
            if res.status_code not in HTTP_OK:
                raise ServerConnectionError(res)

    def __repr__(self):
        return "<ArangoDB connection at '{}'>".format(self.url_prefix)

    @property
    def type(self):
        return self._type

    @property
    def url_prefix(self):
        return '{}://{}:{}/_db/{}'.format(
            self.protocol,
            self.host,
            self.port,
            self._registered_db
        )

    def set_db(self, name):
        """Bind this connection to a specific database.

        :param name: the name of the database
        :type name: str
        """
        self._registered_db = name

    def head(self, endpoint, params=None, headers=None, **kwargs):
        """Execute a HEAD API method.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param params: the request parameters
        :type params: dict | None
        :param headers: the request headers
        :type headers: dict | None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.head(
            url=self.url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def get(self, endpoint, params=None, headers=None, **kwargs):
        """Execute a GET API method.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param params: the request parameters
        :type params: dict | None
        :param headers: the request headers
        :type headers: dict | None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.get(
            url=self.url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self.username, self.password),
        )

    def put(self, endpoint, data=None, params=None, headers=None, **kwargs):
        """Execute a PUT API method.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param data: the request payload
        :type data: str or dict | None
        :param params: the request parameters
        :type params: dict | None
        :param headers: the request headers
        :type headers: dict | None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.put(
            url=self.url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def post(self, endpoint, data=None, params=None, headers=None, **kwargs):
        """Execute a POST API method.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param data: the request payload
        :type data: str or dict | None
        :param params: the request parameters
        :type params: dict | None
        :param headers: the request headers
        :type headers: dict | None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.post(
            url=self.url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def patch(self, endpoint, data=None, params=None, headers=None, **kwargs):
        """Execute a PATCH API method.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param data: the request payload
        :type data: str or dict | None
        :param params: the request parameters
        :type params: dict | None
        :param headers: the request headers
        :type headers: dict | None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.patch(
            url=self.url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def delete(self, endpoint, params=None, headers=None, **kwargs):
        """Execute a DELETE API method.

        :param endpoint: the API endpoint
        :type endpoint: str
        :param params: the request parameters
        :type params: dict | None
        :param headers: the request headers
        :type headers: dict | None
        :returns: the ArangoDB http response
        :rtype: arango.response.Response
        """
        return self.client.delete(
            url=self.url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self.username, self.password)
        )

    def version(self):
        """Return the version of the ArangoDB server.

        :returns: the server version
        :rtype: str
        :raises: VersionGetError
        """
        res = self.get(
            endpoint='/_api/version',
            params={'details': False}
        )
        if res.status_code not in HTTP_OK:
            raise VersionGetError(res)
        return res.body['version']

    def details(self):
        """Return the component details of the ArangoDB server.

        :returns: the server component details
        :rtype: dict
        :raises: VersionGetError
        """
        res = self.get(
            endpoint='/_api/version',
            params={'details': True}
        )
        if res.status_code not in HTTP_OK:
            raise DetailsGetError(res)
        return res.body['details']

    def required_db_version(self):
        """Return the required version of the target database.

        :returns: the required version of the target database
        :rtype: str
        :raises: TargetDatabaseGetError
        """
        res = self.get('/_admin/database/target-version')
        if res.status_code not in HTTP_OK:
            raise TargetDatabaseGetError(res)
        return res.body['version']

    def statistics(self, description=False):
        """Return the server statistics.

        :returns: the statistics information
        :rtype: dict
        :raises: StatisticsGetError
        """
        res = self.get(
            '/_admin/statistics-description'
            if description else '/_admin/statistics'
        )
        if res.status_code not in HTTP_OK:
            raise StatisticsGetError(res)
        res.body.pop('code', None)
        res.body.pop('error', None)
        return res.body

    def role(self):
        """Return the role of the server in the cluster if applicable

        Possible return values are:

        SINGLE:      the server is not in a cluster
        COORDINATOR: the server is a coordinator in the cluster
        PRIMARY:     the server is a primary database in the cluster
        SECONDARY:   the server is a secondary database in the cluster
        UNDEFINED:   in a cluster, UNDEFINED is returned if the server role
                     cannot be determined. On a single server, UNDEFINED is
                     the only possible return value.

        :returns: the server role
        :rtype: str
        :raises: ServerRoleGetError
        """
        res = self.get('/_admin/server/role')
        if res.status_code not in HTTP_OK:
            raise ServerRoleGetError(res)
        return res.body.get('role')

    def time(self):
        """Return the current system time on the server side.

        :returns: the system time
        :rtype: datetime.datetime
        :raises: TimeGetError
        """
        res = self.get('/_admin/time')
        if res.status_code not in HTTP_OK:
            raise TimeGetError(res)
        return datetime.fromtimestamp(res.body['time'])

    def endpoints(self):
        """Return the list of the endpoints the server is listening on.

        Each endpoint is mapped to a list of databases. If the list is empty,
        it means all databases can be accessed via the endpoint. If the list
        contains more than one database, the first database receives all the
        requests by default, unless the name is explicitly specified.

        :returns: the list of endpoints
        :rtype: list
        :raises EndpointsGetError
        """
        res = self.get('/_api/endpoint')
        if res.status_code not in HTTP_OK:
            raise EndpointsGetError(res)
        return res.body

    def echo(self):
        """Return information on the last request (headers, payload etc.)

        :returns: the information on the last request
        :rtype: dict
        :raises: EchoError
        """
        res = self.get('/_admin/echo')
        if res.status_code not in HTTP_OK:
            raise EchoError(res)
        return res.body

    def sleep(self, seconds):
        """Suspend the execution for a specified duration before returning.

        :param seconds: the amount of seconds to suspend
        :type seconds: int
        :returns: the number of seconds suspended
        :rtype: int
        :raises: SleepError
        """
        res = self.get(
            '/_admin/sleep',
            params={'duration': seconds}
        )
        if res.status_code not in HTTP_OK:
            raise SleepError(res)
        return res.body['duration']

    def shutdown(self):
        """Initiate the server shutdown sequence.

        :returns: whether the server was shutdown successfully
        :rtype: bool
        :raises: ShutdownError
        """
        try:
            res = self.delete('/_admin/shutdown')
        except ConnectionError:
            return False
        else:
            if res.status_code not in HTTP_OK:
                raise ShutdownError(res)
            return True

    def run_tests(self, tests):
        """Run the available unittests on the server.

        :param tests: list of files containing the test suites
        :type tests: list
        :returns: the test result
        :rtype: dict
        :raises: TestsRunError
        """
        res = self.post('/_admin/test', data={'tests': tests})
        if res.status_code not in HTTP_OK:
            raise RunTestsError(res)
        return res.body

    def execute(self, program):
        """Execute a javascript program on the server.

        :param program: the body of the javascript program to execute.
        :type program: str
        :returns: the result of the execution
        :rtype: str
        :raises: ProgramExecuteError
        """
        res = self.post(
            '/_admin/execute',
            data=program
        )
        if res.status_code not in HTTP_OK:
            raise ProgramExecuteError(res)
        return res.body

    def read_log(self, upto=None, level=None, start=None, size=None,
                 offset=None, search=None, sort=None):
        """Read the global log from the server.

        The parameters ``upto`` and ``level`` are mutually exclusive.
        The values for ``upto`` or ``level`` must be one of:

            ``fatal`` or 0
            ``error`` or 1
            ``warning`` or 2
            ``info`` or 3 (default)
            ``debug`` or 4
        The parameters ``offset`` and ``size`` can be used for pagination.
        The values for ``sort`` are 'asc' or 'desc'.

        :param upto: return entries up to this level
        :type upto: str or int | None
        :param level: return entries of this level only
        :type level: str or int | None
        :param start: return entries whose id >= to the given value
        :type start: int | None
        :param size: restrict the result to the given value
        :type size: int | None
        :param offset: return entries skipping the given number
        :type offset: int | None
        :param search: return only the entires containing the given text
        :type search: str | None
        :param sort: sort the entries according to their lid values
        :type sort: str | None
        :returns: the server log
        :rtype: dict
        :raises: LogGetError
        """
        params = dict()
        if upto is not None:
            params['upto'] = upto
        if level is not None:
            params['level'] = level
        if start is not None:
            params['start'] = start
        if size is not None:
            params['size'] = size
        if offset is not None:
            params['offset'] = offset
        if search is not None:
            params['search'] = search
        if sort is not None:
            params['sort'] = sort
        res = self.get('/_admin/log')
        if res.status_code not in HTTP_OK:
            LogGetError(res)
        if 'totalAmount' in res.body:
            res.body['total_amount'] = res.body.pop('totalAmount')
        return res.body

    def reload_routing(self):
        """Reload the routing information from the collection ``routing``.

        :returns: whether routing was reloaded successfully
        :rtype: bool
        :raises: RoutingInfoReloadError
        """
        res = self.post('/_admin/routing/reload')
        if res.status_code not in HTTP_OK:
            raise RoutingReloadError(res)
        return not res.body['error']

    #######################
    # Database Management #
    #######################

    def databases(self, user_only=False):
        """"Return the database names.

        :param user_only: list only the databases accessible by the user
        :type user_only: bool
        :returns: the database names
        :rtype: list
        :raises: DatabaseListError
        """
        # Get the current user's databases
        res = self.get(
            '/_api/database/user'
            if user_only else '/_api/database'
        )
        if res.status_code not in HTTP_OK:
            raise DatabaseListError(res)
        return res.body['result']

    def db(self, name):
        """Return the database object.

        :param name: the name of the database
        :type name: str
        :returns: the database object
        :rtype: arango.database.Database
        """
        return Database(self, name)

    def create_database(self, name, users=None):
        """Create a new database.

        :param name: the name of the new database
        :type name: str
        :param users: the users configuration
        :type users: dict
        :returns: the database object
        :rtype: arango.database.Database
        :raises: DatabaseCreateError
        """
        res = self.post(
            '/_api/database',
            data={'name': name, 'users': users}
            if users else {'name': name}
        )
        if res.status_code not in HTTP_OK:
            raise DatabaseCreateError(res)
        return self.db(name)

    def drop_database(self, name, ignore_missing=False):
        """Drop the database of the specified name.

        :param name: the name of the database to delete
        :type name: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: whether the database was dropped successfully
        :rtype: bool
        :raises: DatabaseDeleteError
        """
        res = self.delete('/_api/database/{}'.format(name))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise DatabaseDeleteError(res)
        return not res.body['error']

    ###################
    # User Management #
    ###################

    def users(self):
        """Return details on all users.

        :returns: the mapping of usernames to user information
        :rtype: dict
        :raises: UserListError
        """
        res = self.get('/_api/user')
        if res.status_code not in HTTP_OK:
            raise UserListError(res)
        return {
            record['user']: {
                'user': record['user'],
                'active': record['active'],
                'extra': record['extra'],
                'change_password': record['changePassword']
            } for record in res.body['result']
        }

    def create_user(self, username, password, active=None, extra=None,
                    change_password=None):
        """Create a new user.

        if ``change_password`` is set to true, the only operation allowed by
        the user will be ``self.replace_user`` or ``self.update_user``. All
        other operations executed by the user will result in an HTTP 403.

        :param username: the name of the user
        :type username: str
        :param password: the user password
        :type password: str
        :param active: whether the user is active
        :type active: bool | None
        :param extra: any extra data about the user
        :type extra: dict | None
        :param change_password: whether the user must change the password
        :type change_password: bool | None
        :returns: the information about the new user
        :rtype: dict
        :raises: UserCreateError
        """
        data = {'user': username, 'passwd': password}
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra
        if change_password is not None:
            data['changePassword'] = change_password

        res = self.post('/_api/user', data=data)
        if res.status_code not in HTTP_OK:
            raise UserCreateError(res)
        return {
            'user': res.body['user'],
            'active': res.body['active'],
            'extra': res.body['extra'],
            'change_password': res.body['changePassword'],
        }

    def update_user(self, username, password=None, active=None, extra=None,
                    change_password=None):
        """Update an existing user.

        if ``change_password`` is set to true, the only operation allowed by
        the user will be ``self.replace_user`` or ``self.update_user``. All
        other operations executed by the user will result in an HTTP 403.

        :param username: the name of the existing user
        :type username: str
        :param password: the user password
        :type password: str
        :param active: whether the user is active
        :type active: bool | None
        :param extra: any extra data about the user
        :type extra: dict | None
        :param change_password: whether the user must change the password
        :type change_password: bool | None
        :returns: the information about the updated user
        :rtype: dict
        :raises: UserUpdateError
        """
        data = {}
        if password is not None:
            data['password'] = password
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra
        if change_password is not None:
            data['changePassword'] = change_password

        res = self.patch(
            '/_api/user/{user}'.format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserUpdateError(res)
        return {
            'user': res.body['user'],
            'active': res.body['active'],
            'extra': res.body['extra'],
            'change_password': res.body['changePassword'],
        }

    def replace_user(self, username, password, active=None, extra=None,
                     change_password=None):
        """Replace an existing user.

        if ``change_password`` is set to true, the only operation allowed by
        the user will be ``self.replace_user`` or ``self.update_user``. All
        other operations executed by the user will result in an HTTP 403.

        :param username: the name of the existing user
        :type username: str
        :param password: the user password
        :type password: str
        :param active: whether the user is active
        :type active: bool | None
        :param extra: any extra data about the user
        :type extra: dict | None
        :param change_password: whether the user must change the password
        :type change_password: bool | None
        :returns: the information about the replaced user
        :rtype: dict
        :raises: UserReplaceError
        """
        data = {'user': username, 'password': password}
        if active is not None:
            data['active'] = active
        if extra is not None:
            data['extra'] = extra
        if change_password is not None:
            data['changePassword'] = change_password

        res = self.put(
            '/_api/user/{user}'.format(user=username), data=data
        )
        if res.status_code not in HTTP_OK:
            raise UserReplaceError(res)
        return {
            'user': res.body['user'],
            'active': res.body['active'],
            'extra': res.body['extra'],
            'change_password': res.body['changePassword'],
        }

    def delete_user(self, username, ignore_missing=False):
        """Delete an existing user.

        :param username: the name of the user
        :type username: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :raises: UserDeleteError
        """
        res = self.delete('/_api/user/{user}'.format(user=username))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise UserDeleteError(res)
        return not res.body['error']

    ###################
    # Task Management #
    ###################

    def tasks(self):
        """Return all server tasks that are currently active.

        :returns: server tasks that are currently active
        :rtype: dict
        :raises: TaskGetError
        """
        res = self.get('/_api/tasks')
        if res.status_code not in HTTP_OK:
            raise TasksListError(res)
        return {record['id']: record for record in res.body}

    def task(self, task_id):
        """Return the active server task with the given id.

        :param task_id: the id of the server task
        :type task_id: str
        :returns: the details on the active task
        :rtype: dict
        :raises: TaskGetError
        """
        res = self.get('/_api/tasks/{}'.format(task_id))
        if res.status_code not in HTTP_OK:
            raise TaskGetError(res)
        res.body.pop('code', None)
        res.body.pop('error', None)
        return res.body

    # TODO verify which arguments are optional
    def create_task(self, name, command, params=None, period=None,
                    offset=None, task_id=None):
        """Create a new task with.

        A task can be created with a pre-defined ID which can be specified
        through the ``id`` parameter.

        :param name: the name of the task
        :type name: str
        :param command: the Javascript code to execute
        :type command: str
        :param params: the parameters passed into the command
        :type params: dict
        :param period: the number of seconds between the executions
        :type period: int
        :param offset: the initial delay in seconds
        :type offset: int
        :param task_id: pre-defined ID for the new task
        :type task_id: str
        :return: the details on the new task
        :rtype: dict
        :raises: TaskCreateError
        """
        data = {
            'name': name,
            'command': command,
            'params': params if params else {},
        }
        if task_id is not None:
            data['id'] = task_id
        if period is not None:
            data['period'] = period
        if offset is not None:
            data['offset'] = offset
        res = self.post(
            '/_api/tasks/{}'.format(task_id if task_id else ''),
            data=data
        )
        if res.status_code not in HTTP_OK:
            raise TaskCreateError(res)
        return res.body

    def delete_task(self, task_id, ignore_missing=False):
        """Delete the server task specified by ID.

        :param task_id: the ID of the server task
        :type task_id: str
        :param ignore_missing: ignore HTTP 404
        :type ignore_missing: bool
        :returns: whether the deletion was successful
        :rtype: bool
        :raises: TaskDeleteError
        """
        res = self.delete('/_api/tasks/{}'.format(task_id))
        if res.status_code not in HTTP_OK:
            if not (res.status_code == 404 and ignore_missing):
                raise TaskDeleteError(res)
        return not res.body['error']
