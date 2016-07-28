from __future__ import absolute_import, unicode_literals

import logging
from json import dumps

from six import string_types as string

from arango.http_clients import DefaultHTTPClient

logger = logging.getLogger('arango')


class Connection(object):
    """ArangoDB database connection.

    :param protocol: the internet transfer protocol (default: 'http')
    :type protocol: str
    :param host: ArangoDB host (default: 'localhost')
    :type host: str
    :param port: ArangoDB port (default: 8529)
    :type port: int or str
    :param database: the target database name
    :type database: str
    :param username: ArangoDB username (default: 'root')
    :type username: str
    :param password: ArangoDB password (default: '')
    :type password: str
    :param http_client: the HTTP client
    :type http_client: arango.clients.base.BaseHTTPClient | None
    :param enable_logging: log all API requests
    :type enable_logging: bool

    """

    def __init__(self,
                 protocol='http',
                 host='localhost',
                 port=8529,
                 database='_system',
                 username='root',
                 password='',
                 http_client=None,
                 enable_logging=True):

        self._protocol = protocol.strip('/')
        self._host = host.strip('/')
        self._port = port
        self._database = database or '_system'
        self._url_prefix = '{protocol}://{host}:{port}/_db/{db}'.format(
            protocol=self._protocol,
            host=self._host,
            port=self._port,
            db=self._database
        )
        self._username = username
        self._password = password
        self._client = http_client or DefaultHTTPClient()
        self._logging = enable_logging

    def __repr__(self):
        return '<ArangoDB connection to "{}">'.format(self._url_prefix)

    @property
    def protocol(self):
        """Return the internet transfer protocol.

        :return: the internet transfer protocol
        :rtype: str

        """
        return self._protocol

    @property
    def host(self):
        """Return the ArangoDB host.

        :return: the ArangoDB host
        :rtype: str

        """
        return self._host

    @property
    def port(self):
        """Return the ArangoDB port.

        :return: the ArangoDB port
        :rtype: int

        """
        return self._port

    @property
    def username(self):
        """Return the ArangoDB username.

        :return: the ArangoDB username
        :rtype: int

        """
        return self._username

    @property
    def password(self):
        """Return the ArangoDB user password.

        :return: the ArangoDB user password
        :rtype: int

        """
        return self._password

    @property
    def database(self):
        """"""
        return self._database

    @property
    def client(self):
        return self._client

    @property
    def logging_enabled(self):
        return self._logging

    def handle_request(self, request, handler):
        return handler(getattr(self, request.method)(**request.kwargs))

    def head(self, endpoint, params=None, headers=None, **_):
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
        res = self._client.head(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
        if self._logging:
            logger.debug('HEAD {} {}'.format(endpoint, res.status_code))
        return res

    def get(self, endpoint, params=None, headers=None, **_):
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
        res = self._client.get(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password),
        )
        if self._logging:
            logger.debug('GET {} {}'.format(endpoint, res.status_code))
        return res

    def put(self, endpoint, data=None, params=None, headers=None, **_):
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
        res = self._client.put(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
        if self._logging:
            logger.debug('PUT {} {}'.format(endpoint, res.status_code))
        return res

    def post(self, endpoint, data=None, params=None, headers=None, **_):
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
        res = self._client.post(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
        if self._logging:
            logger.debug('POST {} {}'.format(endpoint, res.status_code))
        return res

    def patch(self, endpoint, data=None, params=None, headers=None, **_):
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
        res = self._client.patch(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
        if self._logging:
            logger.debug('PATCH {} {}'.format(endpoint, res.status_code))
        return res

    def delete(self, endpoint, params=None, headers=None, **_):
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
        res = self._client.delete(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
        if self._logging:
            logger.debug('DELETE {} {}'.format(endpoint, res.status_code))
        return res
