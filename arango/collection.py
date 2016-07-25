from __future__ import absolute_import, unicode_literals

import json

from arango.cursor import Cursor
from arango.constants import HTTP_OK
from arango.exceptions import *
from arango.request import Request
from arango.wrapper import APIWrapper


class BaseCollection(APIWrapper):
    """Base ArangoDB collection.

    :param connection: ArangoDB connection object
    :type connection: arango.connection.Connection
    :param name: the name of the collection
    :type name: str
    """

    TYPES = {
        2: 'document',
        3: 'edge'
    }

    STATUSES = {
        1: 'new',
        2: 'unloaded',
        3: 'loaded',
        4: 'unloading',
        5: 'deleted',
        6: 'loading'
    }

    def __init__(self, connection, name):
        self._conn = connection
        self._name = name

    def __iter__(self):
        """Iterate through the documents in the collection.

        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        res = self._conn.put(
            endpoint='/_api/simple/all',
            data={'collection': self._name}
        )
        if res.status_code not in HTTP_OK:
            raise DocumentFetchError(res)
        return Cursor(self._conn, res.body)

    def __len__(self):
        """Return the number of documents in the collection.

        :returns: the number of documents
        :rtype: int
        :raises: CollectionGetCountError
        """
        res = self._conn.get(
            '/_api/collection/{}/count'.format(self._name)
        )
        if res.status_code not in HTTP_OK:
            raise CollectionGetCountError(res)
        return res.body['count']

    def __getitem__(self, key):
        """Return a document by key from the collection.

        :param key: the document key
        :type key: str
        :returns: the document
        :rtype: dict
        """
        res = self._conn.get(
            '/_api/document/{}/{}'.format(self._name, key)
        )
        if res.status_code in {412, 304}:
            raise DocumentRevisionError(res)
        elif res.status_code == 404:
            return None
        elif res.status_code not in HTTP_OK:
            raise DocumentFetchError(res)
        return res.body

    def __contains__(self, key):
        """Check whether a document is in the collection.

        :param key: the document key
        :type key: str
        :returns: whether the document exists
        :rtype: bool
        :raises: CollectionContainsError
        """
        res = self._conn.head(
            '/_api/document/{}/{}'.format(self._name, key)
        )
        if res.status_code in HTTP_OK:
            return True
        elif res.status_code == 404:
            return False
        raise CollectionContainsError(res)

    def _status(self, code):
        """Return the collection status.

        :param code: the status code
        :type code: int
        :returns: the status text
        :rtype: str
        :raises: CollectionUnknownStatusError
        """
        try:
            return self.STATUSES[code]
        except KeyError:
            raise CollectionUnknownStatusError(
                'received unknown status code {}'.format(code)
            )

    @property
    def name(self):
        """Return the name of the collection.

        :returns: the name of the collection
        :rtype: str
        """
        return self._name

    def rename(self, new_name):
        """Rename the collection.

        :param new_name: the new name for the collection
        :type new_name: str
        :returns: the collection details
        :rtype: dict
        :raises: CollectionRenameError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rename'.format(self._name),
            data={'name': new_name}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRenameError(res)
            self._name = new_name
            return {
                'id': res.body['id'],
                'is_system': res.body['isSystem'],
                'name': res.body['name'],
                'status': self._status(res.body['status']),
                'type': self.TYPES[res.body['type']]
            }

        return request, handler

    def statistics(self):
        """Return the collection statistics.

        :returns: the collection statistics
        :rtype: dict
        :raises: CollectionGetStatisticsError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/figures'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetStatisticsError(res)
            stats = res.body['figures']
            stats['compaction_status'] = stats.pop('compactionStatus', None)
            stats['document_refs'] = stats.pop('documentReferences', None)
            stats['last_tick'] = stats.pop('lastTick', None)
            stats['waiting_for'] = stats.pop('waitingFor', None)
            stats['uncollected_logfile_entries'] = stats.pop(
                'uncollectedLogfileEntries', None
            )
            return stats

        return request, handler

    def revision(self):
        """Return the collection revision.

        :returns: the collection revision
        :rtype: str
        :raises: CollectionGetRevisionError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/revision'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetRevisionError(res)
            return res.body['revision']

        return request, handler

    def properties(self):
        """Return the collection properties.

        :returns: the collection properties
        :rtype: dict
        :raises: CollectionGetPropertiesError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/properties'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetPropertiesError(res)
            result = {
                'id': res.body['id'],
                'name': res.body['name'],
                'edge': res.body['type'] == 3,
                'sync': res.body['waitForSync'],
                'status': self._status(res.body['status']),
                'compact': res.body['doCompact'],
                'system': res.body['isSystem'],
                'volatile': res.body['isVolatile'],
                'journal_size': res.body['journalSize'],
                'keygen': res.body['keyOptions']['type'],
                'user_keys': res.body['keyOptions']['allowUserKeys'],
            }
            if 'increment' in res.body['keyOptions']:
                result['key_increment'] = res.body['keyOptions']['increment']
            if 'offset' in res.body['keyOptions']:
                result['key_offset'] = res.body['keyOptions']['offset']
            return result

        return request, handler

    def set_properties(self, sync=None, journal_size=None):
        """Set the collection properties.

        Only ``sync`` and ``journal_size`` are configurable.

        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :param journal_size: the journal size
        :type journal_size: int
        :returns: the new collection properties
        :rtype: dict
        :raises: CollectionSetPropertiesError
        """

        data = {}
        if sync is not None:
            data['waitForSync'] = sync
        if journal_size is not None:
            data['journalSize'] = journal_size

        request = Request(
            method='put',
            endpoint='/_api/collection/{}/properties'.format(self._name),
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionSetPropertiesError(res)
            result = {
                'id': res.body['id'],
                'name': res.body['name'],
                'edge': res.body['type'] == 3,
                'sync': res.body['waitForSync'],
                'status': self._status(res.body['status']),
                'compact': res.body['doCompact'],
                'system': res.body['isSystem'],
                'volatile': res.body['isVolatile'],
                'journal_size': res.body['journalSize'],
                'keygen': res.body['keyOptions']['type'],
                'user_keys': res.body['keyOptions']['allowUserKeys'],
            }
            if 'increment' in res.body['keyOptions']:
                result['key_increment'] = res.body['keyOptions']['increment']
            if 'offset' in res.body['keyOptions']:
                result['key_offset'] = res.body['keyOptions']['offset']
            return result

        return request, handler

    def load(self):
        """Load the collection into memory.

        :returns: the collection status
        :rtype: str
        :raises: CollectionLoadError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/load'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionLoadError(res)
            return self._status(res.body['status'])

        return request, handler

    def unload(self):
        """Unload the collection from memory.

        :returns: the collection status
        :rtype: str
        :raises: CollectionUnloadError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/unload'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionUnloadError(res)
            return self._status(res.body['status'])

        return request, handler

    def rotate(self):
        """Rotate the collection journal.

        :returns: the result of the operation
        :rtype: dict
        :raises: CollectionRotateJournalError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/rotate'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionRotateError(res)
            return res.body['result']

        return request, handler

    def checksum(self, include_rev=False, include_data=False):
        """Return the collection checksum.

        :param include_rev: include the revision in the checksum calculation
        :type include_rev: bool
        :param include_data: include the data in the checksum calculation
        :type include_data: bool
        :returns: the collection checksum
        :rtype: int
        :raises: CollectionGetChecksumError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/checksum'.format(self._name),
            params={
                'withRevision': include_rev,
                'withData': include_data
            }
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetPropertiesError(res)
            return int(res.body['checksum'])

        return request, handler

    def truncate(self):
        """Truncate the collection.

        :returns: the collection details
        :rtype: dict
        :raises: CollectionTruncateError
        """
        request = Request(
            method='put',
            endpoint='/_api/collection/{}/truncate'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionTruncateError(res)
            return {
                'id': res.body['id'],
                'is_system': res.body['isSystem'],
                'name': res.body['name'],
                'status': self._status(res.body['status']),
                'type': self.TYPES[res.body['type']]
            }

        return request, handler

    def count(self):
        """Return the number of documents in the collection.

        :returns: the number of documents
        :rtype: int
        :raises: CollectionGetCountError
        """
        request = Request(
            method='get',
            endpoint='/_api/collection/{}/count'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise CollectionGetCountError(res)
            return res.body['count']

        return request, handler

    def has(self, key):
        """Check if a document exists by key in the collection.

        :param key: the document key
        :type key: str
        :returns: whether the document exists
        :rtype: bool
        :raises: CollectionContainsError
        """
        request = Request(
            method='head',
            endpoint='/_api/document/{}/{}'.format(self._name, key),
        )

        def handler(res):
            if res.status_code == 200:
                return True
            elif res.status_code == 404:
                return False
            raise CollectionContainsError(res)

        return request, handler

    # TODO look into this endpoint for better documentation and testing
    def export(self, flush=None, max_wait=None, count=None, batch_size=None,
               limit=None, ttl=None, restrict=None):
        """"Export all documents from the collection.

        :param flush: trigger a WAL flush operation prior to the export
        :type flush: bool | None
        :param max_wait: the max wait time in sec for flush operation
        :type max_wait: int | None
        :param count: whether to return the export count
        :type count: bool | None
        :param batch_size: the max number of result documents in one roundtrip
        :type batch_size: int | None
        :param limit: the max number of documents to be included in the cursor
        :type limit: int | None
        :param ttl: time-to-live for the cursor on the server
        :type ttl: int | None
        :param restrict: object with fields to be excluded/included
        :type restrict: dict
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentsExportError
        """
        options = {}
        if flush is not None:
            options['flush'] = flush
        if max_wait is not None:
            options['flushWait'] = max_wait
        if count is not None:
            options['count'] = count
        if batch_size is not None:
            options['batchSize'] = batch_size
        if limit is not None:
            options['limit'] = limit
        if ttl is not None:
            options['ttl'] = ttl
        if restrict is not None:
            options['restrict'] = restrict

        request = Request(
            method='post',
            endpoint='/_api/export',
            params={'collection': self._name},
            data={'options': options} if options else {}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentsExportError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    def fetch(self, filters, offset=None, limit=None):
        """Return all documents that match the given filters.

        :param filters: the document filters
        :type filters: dict
        :param offset: the number of documents to skip
        :type offset: int
        :param limit: the max number of documents to return
        :type limit: int
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        data = {'collection': self._name, 'example': filters}
        if offset is not None:
            data['skip'] = offset
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    def fetch_one(self, filters):
        """Return document that match the given filters.

        DocumentFetchError is raises if there are more than one match.

        :param filters: the document filters
        :type filters: dict
        :returns: the matching document
        :rtype: dict
        :raises: DocumentFetchError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/by-example',
            data={
                'collection': self._name,
                'example': filters,
                'limit': 2,
                'offset': 0
            }
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError('Found more than one document')
            with Cursor(self._conn, res.body) as cursor:
                try:
                    document = cursor.next()
                except StopIteration:
                    return None
                try:
                    cursor.next()
                except StopIteration:
                    return document
                else:
                    raise DocumentFetchError('Found more than one document')

        return request, handler

    def fetch_by_keys(self, keys):
        """Return all documents whose key is found in ``keys``.

        :param keys: the list of document keys
        :type keys: list
        :returns: the list of documents
        :rtype: [dict]
        :raises: DocumentFetchError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/lookup-by-keys',
            data={'collection': self._name, 'keys': keys}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return res.body['documents']

        return request, handler

    def fetch_all(self, offset=None, limit=None):
        """Return all documents in the collection.

        :param offset: the number of documents to skip
        :type offset: int
        :param limit: the max number of documents to return
        :type limit: int
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        data = {'collection': self._name}
        if offset is not None:
            data['skip'] = offset
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/all',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    def fetch_random(self):
        """Return a random document from the collection.

        :returns: a random document
        :rtype: dict
        :raises: DocumentFetchError
        """
        request = Request(
            method='put',
            endpoint='/_api/simple/any',
            data={'collection': self._name}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return res.body['document']

        return request, handler

    def fetch_near(self, latitude, longitude, limit=None):
        """Return documents near a given coordinate.

        By default, at most 100 documents near the coordinate are returned.
        Documents returned are sorted according to distance, with the nearest
        document being the first. If there are documents of equal distance,
        they are be randomly chosen from the set until the limit is reached.
        A geo index must be defined in the collection for this method to be
        used.

        :param latitude: the latitude
        :type latitude: int
        :param longitude: the longitude
        :type longitude: int
        :param limit: the max number of documents to return
        :type limit: int | None
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        full_query = """
        FOR doc IN NEAR(@collection, @latitude, @longitude{})
            RETURN doc
        """.format(', @limit' if limit is not None else '')

        bind_vars = {
            'collection': self._name,
            'latitude': latitude,
            'longitude': longitude
        }
        if limit is not None:
            bind_vars['limit'] = limit

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    def fetch_in_range(self, field, lower, upper, offset=0, limit=100,
                       inclusive=True):
        """Return documents within a given range.

        The returned documents are ordered randomly. A geo index must be
        be defined in the collection for this method to be used.

        :param field: the name of the field to use
        :type field: str
        :param lower: the lower bound
        :type lower: int
        :param upper: the upper bound
        :type upper: int
        :param offset: the number of documents to skip
        :type offset: int | None
        :param limit: the max number of documents to return
        :type limit: int | None
        :param inclusive: whether to include the lower and upper bounds
        :type inclusive: bool
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        if inclusive:
            full_query = """
            FOR doc IN @@collection
                FILTER doc.@field >= @lower && doc.@field <= @upper
                LIMIT @skip, @limit
                RETURN doc
            """
        else:
            full_query = """
            FOR doc IN @@collection
                FILTER doc.@field > @lower && doc.@field < @upper
                LIMIT @skip, @limit
                RETURN doc
            """
        bind_vars = {
            '@collection': self._name,
            'field': field,
            'lower': lower,
            'upper': upper,
            'skip': offset,
            'limit': limit
        }

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    # TODO the WITHIN geo function does not seem to work properly
    def fetch_in_radius(self, latitude, longitude, radius, dist_field=None):
        """Return documents within a given radius.

        The returned documents are ordered randomly. A geo index must be
        defined in the collection to for this method to be used.

        :param latitude: the latitude
        :type latitude: int
        :param longitude: the longitude
        :type longitude: int
        :param radius: the maximum radius
        :type radius: int
        :param dist_field: the key containing the distance
        :type dist_field: str
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        full_query = """
        FOR doc IN WITHIN(@collection, @latitude, @longitude, @radius{})
            RETURN doc
        """.format(', @distance' if dist_field is not None else '')

        bind_vars = {
            'collection': self._name,
            'latitude': latitude,
            'longitude': longitude,
            'radius': radius
        }
        if dist_field is not None:
            bind_vars['distance'] = dist_field

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    def fetch_in_box(self, latitude1, longitude1, latitude2, longitude2,
                     skip=None, limit=None, geo_field=None):
        """Return all documents in an rectangular area.

        A geo index must be defined in the collection for this method to be
        used. If there are more than one geo index, the ``geo`` argument can
        be used to select a particular one.

        :param latitude1: the first latitude
        :type latitude1: int
        :param longitude1: the first longitude
        :type longitude1: int
        :param latitude2: the second latitude
        :type latitude2: int
        :param longitude2: the second longitude
        :type longitude2: int
        :param skip: the number of documents to skip
        :type skip: int
        :param limit: the max number of documents to return
        :type limit: int
        :param geo_field: the field to use (must have geo-index)
        :type geo_field: str
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        data = {
            'collection': self._name,
            'latitude1': latitude1,
            'longitude1': longitude1,
            'latitude2': latitude2,
            'longitude2': longitude2,
        }
        if skip is not None:
            data['skip'] = skip
        if limit is not None:
            data['limit'] = limit
        if geo_field is not None:
            data['geo'] = geo_field

        request = Request(
            method='put',
            endpoint='/_api/simple/within-rectangle',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    def fetch_by_text(self, key, query, limit=None):
        """Return documents that match the specified fulltext ``query``.

        :param key: the key with a fulltext index
        :type key: str
        :param query: the fulltext query
        :type query: str
        :param limit: the max number of documents to return
        :type limit: int
        :returns: the document cursor
        :rtype: arango.cursor.Cursor
        :raises: DocumentFetchError
        """
        full_query = """
        FOR doc IN FULLTEXT(@collection, @field, @query{})
            RETURN doc
        """.format(', @limit' if limit is not None else '')

        bind_vars = {
            'collection': self._name,
            'field': key,
            'query': query
        }
        if limit is not None:
            bind_vars['limit'] = limit

        request = Request(
            method='post',
            endpoint='/_api/cursor',
            data={'query': full_query, 'bindVars': bind_vars}
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return Cursor(self._conn, res.body)

        return request, handler

    ####################
    # Index Management #
    ####################

    def indexes(self):
        """Return the collection indexes.

        :returns: the collection indexes
        :rtype: dict
        :raises: IndexListError
        """
        request = Request(
            method='get',
            endpoint='/_api/index?collection={}'.format(self._name)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexListError(res)

            indexes = {}
            for index_id, details in res.body['identifiers'].items():
                if 'id' in details:
                    del details['id']
                if 'minLength' in details:
                    details['min_length'] = details.pop('minLength')
                if 'byteSize' in details:
                    details['byte_size'] = details.pop('byteSize')
                if 'geoJson' in details:
                    details['geo_json'] = details.pop('geoJson')
                if 'ignoreNull' in details:
                    details['ignore_none'] = details.pop('ignoreNull')
                if 'selectivityEstimate' in details:
                    details['selectivity'] = details.pop('selectivityEstimate')
                if 'isNewlyCreated' in details:
                    details['new'] = details.pop('isNewlyCreated')
                indexes[index_id.split('/', 1)[1]] = details
            return indexes

        return request, handler

    def _add_index(self, data):
        """Helper method for creating a new index."""
        request = Request(
            method='post',
            endpoint='/_api/index?collection={}'.format(self._name),
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexCreateError(res)
            details = res.body
            if 'minLength' in details:
                details['min_length'] = details.pop('minLength')
            if 'byteSize' in details:
                details['byte_size'] = details.pop('byteSize')
            if 'geoJson' in details:
                details['geo_json'] = details.pop('geoJson')
            if 'ignoreNull' in details:
                details['ignore_none'] = details.pop('ignoreNull')
            if 'selectivityEstimate' in details:
                details['selectivity'] = details.pop('selectivityEstimate')
            if 'isNewlyCreated' in details:
                details['new'] = details.pop('isNewlyCreated')
            return details

        return request, handler

    def add_hash_index(self, fields, unique=None, sparse=None):
        """Create a new hash index in the collection.

        :param fields: the document fields to index
        :type fields: list
        :param unique: whether the index is unique
        :type unique: bool | None
        :param sparse: whether to index None's
        :type sparse: bool | None
        :raises: IndexCreateError
        """
        data = {'type': 'hash', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        return self._add_index(data)

    def add_skiplist_index(self, fields, unique=None, sparse=None):
        """Create a new skiplist index in the collection.

        A skiplist index is used to find the ranges of documents (e.g. time).

        :param fields: the document fields to index
        :type fields: list
        :param unique: whether the index is unique
        :type unique: bool | None
        :param sparse: whether to index None's
        :type sparse: bool | None
        :raises: IndexCreateError
        """
        data = {'type': 'skiplist', 'fields': fields}
        if unique is not None:
            data['unique'] = unique
        if sparse is not None:
            data['sparse'] = sparse
        return self._add_index(data)

    def add_geo_index(self, fields, ordered=None, unique=None):
        """Create a geo-spatial index in the collection.

        If ``fields`` only has one value, then a geo-spatial index is created
        using the value the field as the coordinates. The value must be a list
        with at least two doubles: latitude followed by a longitude. Documents
        without the field or with invalid values are ignored.

        If ``fields`` is a list with two values (i.e. a latitude followed by a
        longitude) then a geo-spatial index is created using both. Documents
        without the fields or with invalid values are ignored.

        :param fields: the document fields to index
        :type fields: list
        :param ordered: whether the order is longitude then latitude
        :type ordered: bool | None
        :param unique: whether the index is unique
        :type unique: bool | None
        :raises: IndexCreateError
        """
        data = {'type': 'geo', 'fields': fields}
        if ordered is not None:
            data['geoJson'] = ordered
        if unique is not None:
            data['unique'] = unique
        return self._add_index(data)

    def add_fulltext_index(self, fields, min_length=None):
        """Create a fulltext index to the collection.

        A fulltext index is used to find words or prefixes of words. Only words
        with textual values of minimum length are indexed. Word tokenization is
        done using the word boundary analysis provided by libicu, which uses
        the language selected during server startup. Words are indexed in their
        lower-cased form. The index supports complete match and prefix queries.

        :param fields: the field to index
        :type fields: list
        :param min_length: the minimum number of characters to index
        :type min_length: int
        :raises: IndexCreateError
        """
        if len(fields) > 1:
            raise ValueError('Only one field is currently supported')

        data = {'type': 'fulltext', 'fields': fields}
        if min_length is not None:
            data['minLength'] = min_length
        return self._add_index(data)

    def delete_index(self, index_id):
        """Delete an index from the collection.

        :param index_id: the ID of the index to remove
        :type index_id: str
        :raises: IndexDeleteError
        """
        request = Request(
            method='delete',
            endpoint='/_api/index/{}/{}'.format(self._name, index_id)
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise IndexDeleteError(res)
            return res.body

        return request, handler


class Collection(BaseCollection):
    """ArangoDB collection.

    A collection consists of documents. It is uniquely identified by its name,
    which must consist only of alphanumeric characters, hyphen and underscore
    characters. There are two collection types: "document" and "edge" and the
    default type is "document".

    Be default, collections use the traditional key generator, which generates
    key values in a non-deterministic fashion. A deterministic, auto-increment
    key generator is also available.

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    :param name: the name of the collection
    :type name: str
    """

    def __init__(self, connection, name):
        super(Collection, self).__init__(connection, name)

    def __repr__(self):
        return '<ArangoDB collection "{}">'.format(self._name)

    def fetch_by_key(self, key, rev=None):
        """Fetch a document by key from the collection.

        If ``rev`` is given, its value is compared against the revision of the
        target document. DocumentRevisionError is raised if the revisions do
        not match.

        :param key: the document key
        :type key: str
        :param rev: the document revision
        :type rev: str | None
        :returns: the document or None if not found
        :rtype: dict | None
        :raises: DocumentRevisionError, DocumentFetchError
        """
        request = Request(
            method='get',
            endpoint='/_api/document/{}/{}'.format(self._name, key),
            headers={'If-Match': rev} if rev else {}
        )

        def handler(res):
            if res.status_code in {412, 304}:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return res.body

        return request, handler

    def insert_one(self, document, sync=None):
        """Insert a new document into the collection.

        If the "_key" field is present in ``document``, its value is used as
        the key of the new document. Otherwise, the key is auto-generated.

        :param document: the document body
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the ID, revision and key of the document
        :rtype: dict
        :raises: DocumentInsertError
        """
        params = {'collection': self._name}
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/document',
            data=document,
            params=params
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body

        return request, handler

    def insert_many(self, documents, halt_on_error=True, details=True):
        """Insert multiple documents into the collection in bulk.

        The order of the inserted documents are not retained. If ``details`` is
        set to True, the output will have an additional list of error messages.

        :param documents: the list of documents to insert
        :type documents: list
        :param halt_on_error: halt the operation on a failure
        :type halt_on_error: bool
        :param details: include details of the failures if any
        :type details: bool
        :returns: the result of the operation
        :rtype: dict
        :raises: DocumentsInsertError
        """
        request = Request(
            method='post',
            endpoint='/_api/import',
            data='\r\n'.join([json.dumps(d) for d in documents]),
            params={
                'type': 'documents',
                'collection': self._name,
                'complete': halt_on_error,
                'details': details
            }
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body

        return request, handler

    def update(self, filters, body, limit=None, keep_none=True, sync=None):
        """Update matching document(s) in the collection.

        If ``keep_none`` is set to True, the fields whose value is None are
        retained in the document. Otherwise, the field is removed from the
        document completely.

        :param filters: the filters
        :type filters: dict
        :param body: the document body
        :type body: dict
        :param limit: the max number of documents to return
        :type limit: int
        :param keep_none: keep the fields whose value is None
        :type keep_none: bool
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the number of documents updated
        :rtype: int
        :raises: DocumentUpdateError
        """
        data = {
            'collection': self._name,
            'example': filters,
            'newValue': body,
            'keepNull': keep_none,
        }
        if limit is not None:
            data['limit'] = limit
        if sync is not None:
            data['waitForSync'] = sync

        request = Request(
            method='put',
            endpoint='/_api/simple/update-by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            return res.body['updated']

        return request, handler

    def update_one(self, document, merge=True, keep_none=True, sync=None):
        """Update a document by its key in the collection.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        If ``merge`` is set to True, sub-dictionaries in the document are
        merged rather than replaced.

        If ``keep_none`` is set to True, the fields whose value is None are
        retained in the document. Otherwise, the fields are removed from the
        document completely.

        :param document: the document (partial or full) with the new values
        :type document: dict
        :param merge: merge sub-dictionaries rather than replace
        :type merge: bool
        :param keep_none: keep fields with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool | None
        :returns: the ID, revision and key of the updated document
        :rtype: dict
        :raises: DocumentRevisionError, DocumentUpdateError
        """
        _validate_document(document)

        params = {'keepNull': keep_none, 'mergeObjects': merge}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='patch',
            endpoint='/_api/document/{}/{}'.format(
                self._name, document['_key']
            ),
            data=document,
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            if res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            res.body['_old_rev'] = res.body.pop('_oldRev')
            return res.body

        return request, handler

    def replace(self, filters, body, limit=None, sync=None):
        """Replace matching document(s) in the collection.

        :param filters: the document filters
        :type filters: dict
        :param body: the document body
        :type body: dict
        :param limit: max number of documents to replace
        :type limit: int
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the number of documents replaced
        :rtype: int
        :raises: DocumentReplaceError
        """
        data = {
            'collection': self._name,
            'example': filters,
            'newValue': body
        }
        if limit is not None:
            data['limit'] = limit
        if sync is not None:
            data['waitForSync'] = sync

        request = Request(
            method='put',
            endpoint='/_api/simple/replace-by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            return res.body['replaced']

        return request, handler

    def replace_one(self, document, sync=None):
        """Replace a document by its key in the collection.

        The "_key" field must be present in ``document``. For edge collections,
        The "_from" and "_to" fields must also be present.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        :param document: the new document
        :type document: dict
        :param sync: wait for the replace to sync to disk
        :type sync: bool | None
        :returns: the ID, revision and key of the replaced document
        :rtype: dict
        :raises: DocumentRevisionError, DocumentReplaceError
        """
        _validate_document(document)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='put',
            endpoint='/_api/document/{}/{}'.format(
                self._name, document['_key']
            ),
            params=params,
            data=document,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            res.body['_old_rev'] = res.body.pop('_oldRev')
            return res.body

        return request, handler

    def delete(self, filters, limit=None, sync=None):
        """Delete matching document(s) from the collection.

        :param filters: the document filters
        :type filters: dict
        :param limit: the the max number of documents to delete
        :type limit: int
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the number of documents deleted and ignored
        :rtype: dict
        :raises: DocumentDeleteError
        """
        data = {'collection': self._name, 'example': filters}
        if sync is not None:
            data['waitForSync'] = sync
        if limit is not None:
            data['limit'] = limit

        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-example',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body['deleted']

        return request, handler

    def delete_one(self, document, ignore_missing=True, sync=None):
        """Delete a document by its key from the collection.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        :param document: the document to delete
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :param ignore_missing: ignore missing documents
        :type ignore_missing: bool
        :returns: the ID, revision and key of the replaced document
        :rtype: dict
        :raises: DocumentRevisionError, DocumentDeleteError
        """
        _validate_document(document)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='delete',
            endpoint='/_api/document/{}/{}'.format(
                self._name, document['_key']
            ),
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body

        return request, handler

    def delete_many(self, documents, sync=None):
        """Delete multiple documents from the collection.

        The "_key" field must be present in each document in ``documents``.

        :param documents: list of documents to delete
        :type documents: [dict]
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the number of documents deleted and ignored
        :rtype: dict
        :raises: DocumentDeleteError
        """
        document_keys = []
        for document in documents:
            _validate_document(document)
            document_keys.append(document['_key'])

        data = {
            'collection': self._name,
            'keys': document_keys
        }
        if sync is not None:
            data['waitForSync'] = sync

        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-keys',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return {
                'ignored': res.body['ignored'],
                'removed': res.body['removed']
            }

        return request, handler

    def delete_by_keys(self, keys, sync=None):
        """Delete documents whose keys are in ``keys``.

        :param keys: list of document keys
        :type keys: list
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the number of documents deleted and ignored
        :rtype: dict
        :raises: DocumentDeleteError
        """
        data = {'collection': self._name, 'keys': keys}
        if sync is not None:
            data['waitForSync'] = sync

        request = Request(
            method='put',
            endpoint='/_api/simple/remove-by-keys',
            data=data
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return {
                'ignored': res.body['ignored'],
                'removed': res.body['removed']
            }

        return request, handler


class VertexCollection(BaseCollection):
    """ArangoDB vertex collection.

    A vertex collection consists of vertex documents. It is uniquely identified
    by its name, which must consist only of alphanumeric characters, hyphen and
    the underscore characters. Vertex collections share their namespace with
    other types of collections.

    A vertex document can be of a standard collection or an edge collection.
    The documents in a vertex collection are fully accessible from a standard
    collection. However, accessing documents through a vertex collection gives
    the following guarantees:

    * All modifications are executed in transactions
    * If a vertex is deleted all connected edges are also deleted

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    :param graph: the name of the graph
    :type graph: str
    :param name: the name of the vertex collection
    :type name: str
    """

    _standard_methods = {'name', 'graph_name'}

    def __init__(self, connection, graph, name):
        super(VertexCollection, self).__init__(connection, name)
        self._graph = graph

    def __repr__(self):
        return '<ArangoDB vertex collection "{}" in graph "{}">'.format(
            self._name, self._graph
        )

    @property
    def graph_name(self):
        """Return the name of the graph.

        :returns: the name of the graph
        :rtype: str
        """
        return self._graph

    def fetch_by_key(self, key, rev=None):
        """Fetch a document by key from the vertex collection.

        If ``rev`` is given, its value is compared against the revision of the
        target document. DocumentRevisionError is raised if the revisions do
        not match.

        :param key: the document key
        :type key: str
        :param rev: the document revision
        :type rev: str | None
        :returns: the vertex document or None if not found
        :rtype: dict | None
        :raises: DocumentRevisionError, DocumentFetchError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, key
            ),
            headers={'If-Match': rev} if rev else {}
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return res.body['vertex']

        return request, handler

    def insert_one(self, document, sync=None):
        """Insert a new document into the vertex collection.

        If the "_key" field is present in ``document``, its value is used as
        the key of the new document. Otherwise, the key is auto-generated.

        :param document: the document body
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the ID, revision and key of the document
        :rtype: dict
        :raises: KeyError, DocumentInsertError
        """
        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint='/_api/gharial/{}/vertex/{}'.format(
                self._graph, self._name
            ),
            data=document,
            params=params
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body['vertex']

        return request, handler

    def update_one(self, document, keep_none=True, sync=None):
        """Update a document by its key in the vertex collection.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        If ``keep_none`` is set to True, the fields whose value is None are
        retained in the document. Otherwise, the fields are removed from the
        document completely.

        :param document: the partial/full document with the updated values
        :type document: dict
        :param keep_none: keep fields with value None
        :type keep_none: bool
        :param sync: wait for the update to sync to disk
        :type sync: bool | None
        :returns: the ID, revision and key of the updated document
        :rtype: dict
        :raises: KeyError, DocumentRevisionError, DocumentUpdateError
        """
        _validate_document(document)

        params = {'keepNull': keep_none}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, document['_key']
            ),
            data=document,
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            vertex = res.body['vertex']
            vertex['_old_rev'] = vertex.pop('_oldRev')
            return vertex

        return request, handler

    def replace_one(self, document, sync=None):
        """Replace a document by its key in the vertex collection.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        :param document: the new document
        :type document: dict
        :param sync: wait for operation to sync to disk
        :type sync: bool | None
        :returns: the ID, revision and key of the replaced document
        :rtype: dict
        :raises: KeyError, DocumentRevisionError, DocumentReplaceError
        """
        _validate_document(document)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, document['_key']
            ),
            params=params,
            data=document,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            vertex = res.body['vertex']
            vertex['_old_rev'] = vertex.pop('_oldRev')
            return vertex

        return request, handler

    def delete_one(self, document, ignore_missing=True, sync=None):
        """Delete a document by its key from the vertex collection.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        :param document: the document to delete
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :param ignore_missing: ignore missing documents
        :type ignore_missing: bool
        :returns: whether the document was deleted successfully
        :rtype: bool
        :raises: KeyError, DocumentRevisionError, DocumentDeleteError
        """
        _validate_document(document)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/vertex/{}/{}'.format(
                self._graph, self._name, document['_key']
            ),
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(res)
            if res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body['removed']

        return request, handler


class EdgeCollection(BaseCollection):
    """ArangoDB edge collection.

    An edge collection consists of edge documents. It is uniquely identified
    by its name, which must consist only of alphanumeric characters, hyphen
    and the underscore characters. Edge collections share their namespace with
    other types of collections.

    The documents in an edge collection are fully accessible from a standard
    collection. However, accessing documents through an edge collection gives
    the following guarantees:

    * All modifications are executed in transactions
    * An edge document is checked against the edge definitions on insert

    :param connection: ArangoDB database connection object
    :type connection: arango.connection.Connection
    :param graph: the name of the graph
    :type graph: str
    :param name: the name of the edge collection
    :type name: str
    """

    _standard_methods = {'name', 'graph_name'}

    def __init__(self, connection, graph, name):
        super(EdgeCollection, self).__init__(connection, name)
        self._graph = graph

    def __repr__(self):
        return '<ArangoDB edge collection "{}" in graph "{}">'.format(
            self._name, self._graph
        )

    @property
    def graph_name(self):
        """Return the name of the graph.

        :returns: the name of the graph
        :rtype: str
        """
        return self._graph

    def fetch_by_key(self, key, rev=None):
        """Fetch a document by key from the edge collection.

        If ``rev`` is given, its value is compared against the revision of the
        target document. DocumentRevisionError is raised if the revisions do
        not match.

        :param key: the document key
        :type key: str
        :param rev: the document revision
        :type rev: str | None
        :returns: the vertex document or None if not found
        :rtype: dict | None
        :raises: DocumentRevisionError, DocumentFetchError
        """
        request = Request(
            method='get',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, key
            ),
            headers={'If-Match': rev} if rev else {}
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                return None
            elif res.status_code not in HTTP_OK:
                raise DocumentFetchError(res)
            return res.body["edge"]

        return request, handler

    def insert_one(self, document, sync=None):
        """Insert a new document into the edge collection.

        If the "_key" field is present in ``document``, its value is used as
        the key of the new document. Otherwise, the key is auto-generated.

        The ``document`` must contain the keys "_from" and "_to".

        :param document: the document body
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the id, rev and key of the target document
        :rtype: dict
        :raises: KeyError, DocumentInsertError
        """
        _validate_document(document, required=['_from', '_to'])

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        request = Request(
            method='post',
            endpoint="/_api/gharial/{}/edge/{}".format(
                self._graph, self._name
            ),
            data=document,
            params=params
        )

        def handler(res):
            if res.status_code not in HTTP_OK:
                raise DocumentInsertError(res)
            return res.body["edge"]

        return request, handler

    def update_one(self, document, keep_none=True, sync=None):
        """Update a document by its key in the edge collection.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        If ``keep_none`` is set to True, the fields whose value is None are
        retained in the document. Otherwise, the fields are removed from the
        document completely.

        :param document: the partial/full document with the updated values
        :type document: dict
        :param keep_none: keep fields with value None
        :type keep_none: bool
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the id, rev and key of the target document
        :rtype: dict
        :raises: KeyError, DocumentRevisionError, DocumentUpdateError
        """
        _validate_document(document)

        params = {'keepNull': keep_none}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='patch',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, document['_key']
            ),
            data=document,
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentUpdateError(res)
            edge = res.body["edge"]
            edge['_old_rev'] = edge.pop('_oldRev')
            return edge

        return request, handler

    def replace_one(self, document, sync=None):
        """Replace a document by its key in the edge collection.

        The "_key", "_from" and "_to" fields must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        :param document: the new document
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :returns: the id, rev and key of the target document
        :rtype: dict
        :raises: KeyError, DocumentRevisionError, DocumentReplaceError
        """
        _validate_document(document)

        headers, params = {}, {}
        if sync is not None:
            params['waitForSync'] = sync

        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='put',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, document['_key']
            ),
            data=document,
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentReplaceError(res)
            edge = res.body["edge"]
            edge['_old_rev'] = edge.pop('_oldRev')
            return edge

        return request, handler

    def delete_one(self, document, ignore_missing=False, sync=None):
        """Delete a document from the collection by its key.

        The "_key" field must be present in ``document``.

        If the "_rev" field is present in ``document``, its value is compared
        against the revision of the target document. DocumentRevisionError is
        raised if the revisions do not match.

        :param document: the document to delete
        :type document: dict
        :param sync: wait for the operation to sync to disk
        :type sync: bool | None
        :param ignore_missing: ignore missing documents
        :type ignore_missing: bool
        :returns: whether the document was deleted successfully
        :rtype: bool
        :raises: KeyError, DocumentRevisionError, DocumentDeleteError
        """
        _validate_document(document)

        params = {}
        if sync is not None:
            params['waitForSync'] = sync

        headers = {}
        revision = document.get('_rev')
        if revision is not None:
            headers['If-Match'] = revision

        request = Request(
            method='delete',
            endpoint='/_api/gharial/{}/edge/{}/{}'.format(
                self._graph, self._name, document['_key']
            ),
            params=params,
            headers=headers
        )

        def handler(res):
            if res.status_code == 412:
                raise DocumentRevisionError(res)
            elif res.status_code == 404:
                if ignore_missing:
                    return False
                raise DocumentDeleteError(res)
            elif res.status_code not in HTTP_OK:
                raise DocumentDeleteError(res)
            return res.body['removed']

        return request, handler


def _validate_document(document, required=None):
    """Check if ``document`` contains the fields in ``required``.

    :param document: the document body
    :type document: dict
    :param required: list of required field names
    :type required: list
    :raises: KeyError
    """
    required = required if required else ['_key']
    missing = [f for f in required if f not in document]
    if missing:
        raise KeyError('The document is missing fields {}'.format(missing))
