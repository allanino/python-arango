.. python-arango documentation master file, created by
   sphinx-quickstart on Sun Jul 24 17:17:48 2016.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to python-arango's documentation!
=========================================

Project python-arango_ is a Python driver for ArangoDB_'s REST API.

It is compatible with Python versions 2.7+ and 3.4+.

.. _python-arango: https://github.com/joowani/python-arango
.. _ArangoDB: https://www.arangodb.com


Installation
------------

To install a stable version from PyPi_:

.. code-block:: bash

    # You may need to use sudo depending on your environment
    pip install python-arango


To install the latest version directly from GitHub:

.. code-block:: bash

    git clone https://github.com/joowani/python-arango.git
    cd python-arango
    python2.7 setup.py install

.. _PyPi: https://pypi.python.org/pypi


Getting Started
---------------

Python-Arango's github page has a few examples you can look at here_.

.. _here: https://github.com/joowani/python-arango


Contents
--------

.. toctree::
    :maxdepth: 2

    client
    connection
    database
    collection
    graph
    query
    cursor
    async
    batch
    transaction
    exceptions



Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

