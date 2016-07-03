from __future__ import absolute_import, unicode_literals

from functools import wraps


class APIWrapper(object):
    """ArangoDB API wrapper."""

    _bypass_methods = {'name'}

    def __getattribute__(self, attr):
        method = object.__getattribute__(self, attr)
        if attr.startswith('_') or attr in self._bypass_methods:
            return method

        conn = object.__getattribute__(self, '_conn')

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            request, handler = method(*args, **kwargs)
            return conn.handle_request(request, handler)

        return wrapped_method
