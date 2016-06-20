from __future__ import absolute_import, unicode_literals

from functools import wraps


class APIWrapper(object):
    """ArangoDB API wrapper.

    This is the base class for the following:
    - arango.collection.Collection
    - arango.graph.Graph
    """

    _plain_methods = {'name'}

    def __getattribute__(self, attr):
        method = object.__getattribute__(self, attr)
        if attr.startswith('_') or attr in self._plain_methods:
            return method

        conn = object.__getattribute__(self, '_conn')
        if conn.type == 'normal':

            @wraps(method)
            def wrapped_method(*args, **kwargs):
                req, handler = method(*args, **kwargs)
                res = getattr(conn, req.method)(**req.kwargs)
                return handler(res)

            return wrapped_method

        elif conn.type == 'batch':

            @wraps(method)
            def wrapped_method(*args, **kwargs):
                req, handler = method(*args, **kwargs)
                conn.add(req, handler)
                return True

            return wrapped_method

        elif conn.type == 'transaction':
            pass
