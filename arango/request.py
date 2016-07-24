from __future__ import absolute_import, unicode_literals

from json import dumps

from six import moves


class Request(object):
    """ArangoDB API request object.


    """

    __slots__ = ('method', 'endpoint', 'headers', 'params', 'data')

    def __init__(self, method, endpoint, headers=None, params=None, data=None):
        self.method = method
        self.endpoint = endpoint
        self.headers = headers or {}
        self.params = params or {}
        self.data = data or {}

    def __repr__(self):
        return "<ArangoDB request '{} {}'>".format(
            self.method.upper(), self.endpoint
        )

    @property
    def kwargs(self):
        return {
            'endpoint': self.endpoint,
            'headers': self.headers,
            'params': self.params,
            'data': self.data,
        }

    def stringify(self):
        path = self.endpoint
        if self.params:
            path += "?" + moves.urllib.parse.urlencode(self.params)
        request_string = "{} {} HTTP/1.1".format(self.method, path)
        if self.headers:
            for key, value in self.headers.items():
                request_string += "\r\n{key}: {value}".format(
                    key=key, value=value
                )
        if self.data:
            request_string += "\r\n\r\n{}".format(dumps(self.data))
        return request_string
