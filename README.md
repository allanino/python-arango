Python-Arango
=============

**Python-Arango** is a Python driver for 
[ArangoDB](<https://www.arangodb.com/>)'s REST API. 

Compatibility
-------------

- Python versions 2.7+ or 3.4+
- ArangoDB server version 3.0+

Installation
------------

- To install a stable version from [PyPi](<https://pypi.python.org/pypi>):

```bash
# You may need to sudo depending on your environment
pip install python-arango
```

- To install the latest version from 
[GitHub](<https://github.com/joowani/python-arango>):

```bash
git clone https://github.com/joowani/python-arango.git
cd python-arango

# You may need to sudo depending on your environment
python setup.py install
```

Client Initialization
---------------------

```python
from arango import ArangoClient

client = ArangoClient(
    protocol='http',
    host="localhost", 
    port=8529,
    username='root'
)
```

Databases
---------

```python
from arango import ArangoClient

client = ArangoClient()

# List databases
client.databases()

# Create a database
db = client.create_database('school')

# Retrieve database information
db.properties()

# Delete a database
client.delete_database('school')
```

Collections
-----------

```python
from arango import ArangoClient

client = ArangoClient()
db = client.db('school')

# List collections
db.collections()

# Create a collection
students = db.create_collection('students')

# Retrieve collection information
students.properties()
students.revision()
students.statistics()
students.checksum()
students.count()

# Perform actions on a collection
students.load()
students.unload()
students.truncate()
students.set_properties(journal_size=3000000)
```

Documents
---------

```python
from arango import ArangoClient

client = ArangoClient()
db = client.db('school')
students = db.collection('students')

lola = {'_key': '1', 'GPA': 3.5, 'first': 'Lola', 'last': 'Martin'}
abby = {'_key': '2', 'GPA': 3.2, 'first': 'Abby', 'last': 'Page'}
john = {'_key': '3', 'GPA': 3.6, 'first': 'John', 'last': 'Kim'} 
emma = {'_key': '4', 'GPA': 4.0, 'first': 'Emma', 'last': 'Park'}

# Insert a new document
result = students.insert_one(lola)
print(result['_id'], result['_key'], result['_rev'])

# Retrieve information on the documents in the collection
print(students.has('1'))
print('3' in students)
print(students.count())
print(len(students) > 5)

# Insert multiple documents in bulk
students.insert_many([abby, john, emma])

# Fetch a single document by filters
students.fetch_one({'first': 'Emma'})

# Fetch documents by filters
for student in students.fetch({'first': 'John'}):
    print(student['_key'], student['GPA'])

# Fetch a single document by key
students.fetch_by_key('5')

# Fetch multiple documents by key
students.fetch_by_keys(['1', '2'])

# Update documents by filters
students.update({'last': 'Simmons'}, {'GPA': 3.0})

# Update a single document
lola['GPA'] = 2.6
students.update_one(lola)

# Replace documents by filters
becky = {'first': 'Becky', 'last': 'Hamilton', 'GPA': '3.3'}
students.replace({'first': 'Emma'}, becky)

# Replace a single document
emma['GPA'] = 3.1
students.replace_one(emma)

# Iterate through all documents and update
for student in students:
    student['GPA'] = 4.0
    student['happy'] = True
    students.update_one(student)
```

Managing Indexes
----------------

```python
from arango import ArangoClient

client = ArangoClient()
db = client.db('school')
students = db.collection('students')

# List the indexes
students.indexes()

# Add a new hash index on fields 'first' and 'last'
students.add_hash_index(fields=['first', 'last'], unique=True)

# Add a new fulltext index on fields 'first' and 'last'
students.add_fulltext_index(fields=['first'])
students.add_fulltext_index(fields=['last'])

# Add a new skiplist index on field 'GPA'
students.add_skiplist_index(fields=['GPA'], sparse=False)
```

Graphs, Vertices and Edges
--------------------------

```python
from arango import ArangoClient

client = ArangoClient()
db = client.db('school')

# List graphs
db.graphs()

# Create a graph
schedule = db.create_graph('schedule')

# Create vertex and edge collections for the graph
professors = schedule.create_vertex_collection('professors')
classes = schedule.create_vertex_collection('classes')
teaches = schedule.create_edge_collection(
    name='teaches',
    from_collections=['professors'],
    to_collections=['classes']
)

# Retrieve graph information
schedule.properties()

# List orphan collections (no edges)
schedule.orphan_collections()

# List edge collections
schedule.edge_collections()

# List vertex collections
schedule.vertex_collections()

# Insert vertices
professors.insert_one({'_key': 'michelle', 'name': 'Professor Michelle'})
classes.insert_one({'_key': 'CSC101', 'name': 'Introduction to CS'})
classes.insert_one({'_key': 'MAT223', 'name': 'Linear Algebra'})
classes.insert_one({'_key': 'STA201', 'name': 'Introduction to Statistics'})

# Insert edges
teaches.insert_one({'_from': 'professors/michelle', '_to': 'classes/CSC101'})
teaches.insert_one({'_from': 'professors/michelle', '_to': 'classes/STA201'})
teaches.insert_one({'_from': 'professors/michelle', '_to': 'classes/MAT223'})

# Traverse the graph
result = schedule.traverse(start_vertex='professors/michelle')
print(result['visited'])
```

AQL: Queries, Functions and Cache
---------------------------------

```python
from arango import ArangoClient

client = ArangoClient()
db = client.db('school')

# Retrieve the execution plan without executing it
db.query.explain('FOR student IN students RETURN student')

# Validate the query without executing it
db.query.validate('FOR student IN students RETURN student')

# Execute the query and iterate through the result cursor
result = db.query.execute(
  'FOR student IN students FILTER student.GPA < @value RETURN student',
  bind_vars={'value': 3.2}
)
for student in result:  
  print(student['_key'], student['first'], student['last'])
  
# List AQL functions
db.query.functions()

# Create a new AQL function
db.query.create_function(
  name='myfunctions::temperature::ctof',
  code='function (celsius) { return celsius * 1.8 + 32; }'
)

# Delete an AQL function
db.query.delete_function('myfunctions::temperature::ctof')

# Manage the query cache
db.query.cache.clear()
db.query.cache.set_properties(mode='demand', limit=10000)
db.query.cache.properties()
```

Asynchronous Requests
---------------------

```python
from arango import ArangoClient

client = ArangoClient()

db = client.db('school').async(return_result=True)
students = db.collection('students')
job1 = students.insert_one({'_key': '1', 'name': 'Lola'})
job2 = students.insert_one({'_key': '2', 'name': 'Abby'})
job3 = students.insert_one({'_key': '3', 'name': 'John'})
job4 = students.insert_one({'_key': '4', 'name': 'Emma'})

print(job1.result())
print(job2.result())
print(job3.result())
print(job4.result())
```


Batch Requests
--------------

```python
from arango import ArangoClient

client = ArangoClient()

with client.db('school').batch(return_result=True) as db:
    students = db.collection('students')
    job1 = students.insert_one({'_key': '1', 'name': 'Lola'})
    job2 = students.insert_one({'_key': '2', 'name': 'Abby'})
    job3 = students.insert_one({'_key': '3', 'name': 'John'})
    job4 = students.insert_one({'_key': '4', 'name': 'Emma'})

print(job1.result())
print(job2.result())
print(job3.result())
print(job4.result())
```

Transactions
------------

*Coming in the next release*

Administration and Monitoring
-----------------------------
```python
from arango import ArangoClient

client = ArangoClient()

# Manage users
client.users()  # list users 
client.create_user(username='jay', password='green')
client.update_user(username='jay', password='blue', change_password=True)
client.replace_user(username='jay', password='red', extra={'dept': 'IT'})
client.grant_user_access(username='jay', database='students')
client.revoke_user_access(username='jay', database='students')
client.delete_user(username='jay')

# Retrieve the server information
client.version()
client.required_db_version()
client.time()
client.role()
client.statistics()
client.read_log(level="debug")
client.endpoints()
client.echo()

# Perform actions on the server (requires access to '_system' DB)
client.sleep(seconds=2)
client.shutdown()
client.reload_routing()

# Manage the write-ahead log (WAL)
client.wal.set_properties(oversized_ops=10000)
client.wal.properties()
client.wal.transactions()
client.wal.flush(garbage_collect=True)
```
