# Name of the default ArangoDB database
DEFAULT_DB = '_system'

# Valid collection types
COLLECTION_TYPES = {
    2: 'document',
    3: 'edge'
}

# Valid collection statuses
COLLECTION_STATUSES = {
    1: 'new',
    2: 'unloaded',
    3: 'loaded',
    4: 'unloading',
    5: 'deleted',
    6: 'loading'
}

# HTTP OK status codes
HTTP_OK = {
    200, '200',
    201, '201',
    202, '202',
    203, '203',
    204, '204',
    205, '205',
    206, '206',
}

# ArangoDB specific logging levels
LOG_LEVELS = {
    'fatal': 0,
    'error': 1,
    'warning': 2,
    'info': 3,
    'debug': 4,
}

# ArangoDB specific log sorting types
LOG_SORTING_TYPES = {'asc', 'desc'}
