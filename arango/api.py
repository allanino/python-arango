from __future__ import absolute_import, unicode_literals

from functools import wraps


class APIWrapper(object):
    """ArangoDB API wrapper."""

    _internal_methods = {'name'}

    def __getattribute__(self, attr):
        method = object.__getattribute__(self, attr)
        internal = object.__getattribute__(self, '_internal_methods')

        if attr in internal or attr.startswith('_') or attr.isupper():
            return method
        conn = object.__getattribute__(self, '_conn')

        @wraps(method)
        def wrapped_method(*args, **kwargs):
            request, handler = method(*args, **kwargs)
            return conn.handle_request(request, handler)

        return wrapped_method
