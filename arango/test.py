from arango.batch import Batch
from arango.connection import Connection

conn = Connection()
db = conn.db('_system')

batch = db.batch()
batch.collection('test').properties()
batch.collection('test').revision()
batch.collection('test').rotate()
batch.collection('test').set_properties(sync=True)
batch.collection('test').insert({'_key': '1', 'val': 1})
batch.collection('test').insert({'_key': '2', 'val': 2})
batch.collection('test').insert({'_key': '3', 'val': 3})
print(batch.commit())


# print(batch._requests)
# print(batch.commit())
