"""ArangoDB Exceptions."""


class ArangoError(Exception):
    """Base class for ArangoDB request errors.

    :param response: the response object
    :type response: arango.response.Response
    """

    def __init__(self, response):
        # Get the ArangoDB error message if given
        if response.body is not None and "errorMessage" in response.body:
            message = response.body["errorMessage"]
        elif response.status_text is not None:
            message = response.status_text
        else:
            message = "request failed"

        # Get the ArangoDB error number if given
        if response.body is not None and "errorNum" in response.body:
            self.error_code = response.body["errorNum"]
        else:
            self.error_code = None

        # Generate the error message for the exception
        super(ArangoError, self).__init__(message)
        self.message = message
        self.method = response.method
        self.url = response.url
        self.http_code = response.status_code


class ServerConnectionError(ArangoError):
    """Failed to connect to ArangoDB."""


###########################
# Miscellaneous Functions #
###########################


class EndpointsGetError(ArangoError):
    """Failed to get the endpoints."""


class VersionGetError(ArangoError):
    """Failed to retrieve the server version."""


class DetailsGetError(ArangoError):
    """Failed to retrieve the server details."""


class WriteAheadLogFlushError(ArangoError):
    """Failed to flush the write-ahead log."""


class WriteAheadLogGetError(ArangoError):
    """Failed to get the write-ahead log."""


class WriteAheadLogSetError(ArangoError):
    """Failed to configure the write-ahead log."""


class TimeGetError(ArangoError):
    """Failed to return the current system time."""


class EchoError(ArangoError):
    """Failed to return current request."""


class SleepError(ArangoError):
    """Failed to suspend the execution."""


class TargetDatabaseGetError(ArangoError):
    """Failed to retrieve the required database version."""


class ShutdownError(ArangoError):
    """Failed to initiate a clean shutdown sequence."""


class RunTestsError(ArangoError):
    """Failed to execute the specified tests on the server."""


class ProgramExecuteError(ArangoError):
    """Failed to execute a the given Javascript program."""


#########
# Tasks #
#########


class TasksListError(ArangoError):
    """Failed to list the active server tasks."""


class TaskGetError(ArangoError):
    """Failed to get the active server task."""


class TaskCreateError(ArangoError):
    """Failed to create a server task."""


class TaskDeleteError(ArangoError):
    """Failed to delete a server task."""


#######################
# Database Exceptions #
#######################


class DatabaseListError(ArangoError):
    """Failed to get the list of databases."""


class DatabaseOptionsGetError(ArangoError):
    """Failed to get the database options."""


class DatabaseGetError(ArangoError):
    """Failed to get the database."""


class DatabaseCreateError(ArangoError):
    """Failed to create the database."""


class DatabaseDeleteError(ArangoError):
    """Failed to delete the database."""


###################
# User Exceptions #
###################


class UserListError(ArangoError):
    """Failed to get the list of users."""


class UserCreateError(ArangoError):
    """Failed to create the user."""


class UserUpdateError(ArangoError):
    """Failed to update the user."""


class UserReplaceError(ArangoError):
    """Failed to replace the user."""


class UserDeleteError(ArangoError):
    """Failed to delete the user."""


#########################
# Collection Exceptions #
#########################


class CollectionNotFoundError(ArangoError):
    """Failed to find the collection."""


class CollectionListError(ArangoError):
    """Failed to get the list of collections."""


class CollectionGetCountError(ArangoError):
    """Failed to get the count of the documents in the collections."""


class CollectionGetPropertiesError(ArangoError):
    """Failed to get the collection properties."""


class CollectionSetPropertiesError(ArangoError):
    """Failed to set the collection properties."""


class CollectionGetStatisticsError(ArangoError):
    """Failed to get the collection statistics."""


class CollectionGetRevisionError(ArangoError):
    """Failed to get the collection revision."""


class CollectionGetChecksumError(ArangoError):
    """Failed to get the collection checksum."""


class CollectionCreateError(ArangoError):
    """Failed to create the collection."""


class CollectionDropError(ArangoError):
    """Failed to delete the collection"""


class CollectionUpdateError(ArangoError):
    """Failed to update the collection."""


class CollectionRenameError(ArangoError):
    """Failed to rename the collection."""


class CollectionTruncateError(ArangoError):
    """Failed to truncate the collection."""


class CollectionLoadError(ArangoError):
    """Failed to load the collection into memory."""


class CollectionUnloadError(ArangoError):
    """Failed to unload the collection from memory."""


class CollectionRotateError(ArangoError):
    """Failed to rotate the journal of the collection."""


########################################
# Documents Import & Export Exceptions #
########################################


class DocumentsExportError(ArangoError):
    """Failed to bulk export documents/edges."""


#######################
# Document Exceptions #
#######################


class DocumentInvalidError(ArangoError):
    """The document is invalid (malformed)."""


class DocumentRevisionError(ArangoError):
    """The expected and actual document revisions do not match."""


class DocumentGetError(ArangoError):
    """Failed to get the document."""


class DocumentInsertError(ArangoError):
    """Failed to insert the document."""


class DocumentInsertManyError(ArangoError):
    """Failed to insert the documents in bulk."""


class DocumentReplaceError(ArangoError):
    """Failed to replace the document."""


class DocumentUpdateError(ArangoError):
    """Failed to update the document."""


class DocumentDeleteError(ArangoError):
    """Failed to delete the document."""


###################
# Edge Exceptions #
###################


class EdgeRevisionError(ArangoError):
    """The expected and actual edge revisions do not match."""


class EdgeGetError(ArangoError):
    """Failed to get the edge."""


class EdgeCreateError(ArangoError):
    """Failed to create the edge."""


class EdgeReplaceError(ArangoError):
    """Failed to replace the edge."""


class EdgeUpdateError(ArangoError):
    """Failed to update the edge."""


class EdgeDeleteError(ArangoError):
    """Failed to delete the edge."""


#####################
# Vertex Exceptions #
#####################


class VertexInvalidError(ArangoError):
    """The vertex is invalid (malformed)."""


class VertexRevisionError(ArangoError):
    """The expected and actual vertex revisions do not match."""


class VertexGetError(ArangoError):
    """Failed to get the vertex."""


class VertexCreateError(ArangoError):
    """Failed to create the vertex."""


class VertexUpdateError(ArangoError):
    """Failed to update the vertex."""


class VertexReplaceError(ArangoError):
    """Failed to replace the vertex."""


class VertexDeleteError(ArangoError):
    """Failed to delete the vertex."""


####################
# Index Exceptions #
####################


class IndexListError(ArangoError):
    """Failed to get the list of indexes."""


class IndexCreateError(ArangoError):
    """Failed to create the index."""


class IndexDeleteError(ArangoError):
    """Failed to delete the index."""


################################
# Query Query & Cache Exceptions #
################################


class QueryExplainError(ArangoError):
    """Failed to explain the Query query."""


class QueryValidateError(ArangoError):
    """Failed to validate the Query query."""


class AQLQueryExecuteError(ArangoError):
    """Failed to execute the Query query."""


class AQLQueryCacheClearError(ArangoError):
    """Failed to clear the Query query cache."""


class AQLQueryCacheGetError(ArangoError):
    """Failed to get the Query query cache properties."""


class AQLQueryCacheConfigureError(ArangoError):
    """Failed to configure the Query query cache properties."""


#####################
# Cursor Exceptions #
#####################

class CursorGetNextError(ArangoError):
    """Failed to get the next cursor result."""


class CursorDeleteError(ArangoError):
    """Failed to delete the cursor."""


###########################
# Query Function Exceptions #
###########################


class AQLFunctionListError(ArangoError):
    """Failed to get the list of Query functions."""


class AQLFunctionCreateError(ArangoError):
    """Failed to create the Query function."""


class AQLFunctionDeleteError(ArangoError):
    """Failed to delete the Query function."""


###########################
# Simple Query Exceptions #
###########################


class DocumentFindManyError(ArangoError):
    """Failed to execute the ``by-example`` simple query."""


class DocumentFindOneError(ArangoError):
    """Failed to execute the ``first-example`` simple query."""


class DocumentReplaceManyError(ArangoError):
    """Failed to execute the ``replace-by-example`` simple query."""


class DocumentFindAndUpdateError(ArangoError):
    """Failed to execute the ``update-by-example`` simple query."""


class DocumentDeleteManyError(ArangoError):
    """Failed to execute the ``Delete-by-example`` simple query."""


class DocumentGetFirstError(ArangoError):
    """Failed to execute the ``first`` simple query."""


class DocumentGetLastError(ArangoError):
    """Failed to execute the ``last`` simple query."""


class DocumentGetAllError(ArangoError):
    """Failed to execute the `all`` simple query."""


class DocumentGetRandomError(ArangoError):
    """Failed to execute the ``any`` simple query."""


class DocumentFindInRangeError(ArangoError):
    """Failed to execute the ``range`` simple query."""


class DocumentFindNearError(ArangoError):
    """Failed to execute the ``near`` simple query."""


class DocumentFindInRadiusError(ArangoError):
    """Failed to execute the ``within`` simple query."""


class DocumentFindInRectangleError(ArangoError):
    """Failed to execute the ``within-rectangle`` simple query."""


class DocumentFindTextError(ArangoError):
    """Failed to execute the ``fulltext`` simple query."""


class SimpleQueryError(ArangoError):
    """Failed to execute a simple query."""


##########################
# Transaction Exceptions #
##########################


class TransactionExecuteError(ArangoError):
    """Failed to execute a transaction."""


class TransactionGetError(ArangoError):
    """Failed to get the running transactions."""


####################
# Batch Exceptions #
####################


class BatchExecuteError(ArangoError):
    """Failed to execute a batch request."""


####################
# Graph Exceptions #
####################


class GraphListError(ArangoError):
    """Failed to get the list of graphs."""


class GraphGetError(ArangoError):
    """Failed to get the graph."""


class GraphCreateError(ArangoError):
    """Failed to create the graph."""


class GraphDropError(ArangoError):
    """Failed to delete the graph."""


class GraphPropertyError(ArangoError):
    """Failed to get the graph property."""


class GraphTraversalError(ArangoError):
    """Failed to execute the graph traversal."""


################################
# Vertex Collection Exceptions #
################################


class VertexCollectionListError(ArangoError):
    """Failed to get the list of vertex collections."""


class VertexCollectionCreateError(ArangoError):
    """Failed to create the vertex collection."""


class VertexCollectionDropError(ArangoError):
    """Failed to delete the vertex collection."""


#########################################
# Edge Collection/Definition Exceptions #
#########################################


class EdgeDefinitionListError(ArangoError):
    """Failed to get the list of edge definitions."""


class EdgeDefinitionCreateError(ArangoError):
    """Failed to create the edge definition."""


class EdgeDefinitionReplaceError(ArangoError):
    """Failed to replace the edge definition."""


class EdgeDefinitionDeleteError(ArangoError):
    """Failed to delete the edge definition."""


##########################################
# Administration & Monitoring Exceptions #
##########################################


class LogGetError(ArangoError):
    """Failed to get the global log."""


class RoutingReloadError(ArangoError):
    """Failed to reload the routing information."""


class StatisticsGetError(ArangoError):
    """Failed to get the server statistics."""


class StatisticsDescriptionGetError(ArangoError):
    """Failed to get the statistics description."""


class ServerRoleGetError(ArangoError):
    """Failed to get the role of the server in a cluster."""


