from __future__ import absolute_import, unicode_literals

from json import dumps

from six import string_types as string

from arango.http_clients import DefaultHTTPClient


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
    :param client: the HTTP client
    :type client: arango.clients.base.BaseHTTPClient | None
    """

    def __init__(self,
                 protocol='http',
                 host='localhost',
                 port=8529,
                 database='_system',
                 username='root',
                 password='',
                 client=None):

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
        self._client = client or DefaultHTTPClient()
        self._execute_type = 'normal'

    def __repr__(self):
        return '<ArangoDB connection to "{}">'.format(self.url_prefix)

    @property
    def protocol(self):
        return self._protocol

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def username(self):
        return self._username

    @property
    def password(self):
        return self._password

    @property
    def database(self):
        return self._database

    @property
    def client(self):
        return self._client

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
        return self._client.head(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

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
        return self._client.get(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password),
        )

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
        return self._client.put(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

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
        return self._client.post(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

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
        return self._client.patch(
            url=self._url_prefix + endpoint,
            data=data if isinstance(data, string) else dumps(data),
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )

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
        return self._client.delete(
            url=self._url_prefix + endpoint,
            params=params,
            headers=headers,
            auth=(self._username, self._password)
        )
