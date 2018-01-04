# Maria Management

The strom project takes advantage of SQL look up speeds to store the dstream templates in a
Maria database. The methods for interacting with the Maria database are written using the
PyMySQL pure python client library and are called by the coordinator.

## Using the PyMySQL Wrapper for MariaDB

Project Github: https://github.com/PyMySQL/PyMySQL

MariaDB is the open source branch of MySQL, and behaves almost exactly the same. PyMySQL
is a pure python open source client library for MySQL.

### General Structure of a Maria Method

All of the methods for interacting with the Maria database follow the same general
pattern:

  1. Build the SQL query string with correct string interpolation
  2. Execute the query using the execute or executemany method on the cursor object
  3. Return the data on the cursor object as a result of a retrieval or an attribute
     like rowcount to verify a successful query execution.

The first task is the most difficult. The execute and executemany functions take as their
first argument a SQL query string and as their second a tuple or list of values. The two
execute functions use Python string interpolation to sub in the values from the tuple of
values to complete the full SQL query.

A common mistake is including table or column names in the tuple of values for the execute
functions in anticipation of string interpolation working as expected. Doing this will
result in an error.

If a user wants to abstract away the name of the table or columns for a SQL query using
PyMySQL, then they must do so prior to calling the execute method. The string interpolation
for the table and column names must be done when formulating the query string like below:

```
def _insert_filtered_measure_into_stream_lookup_table(self, stream_token, filtered_measure, value, unique_id):
    stringified_stream_token_uuid = _stringify_uuid(stream_token)
    query = ("UPDATE `%s` SET %s " % (stringified_stream_token_uuid, filtered_measure)) + "= %s WHERE unique_id = %s"
    // Using string interpolation for the table and column name in the construction of the SQL query before the
    // self.execute() call.
    // Note that the string interpolation for the values themselves will still be interpolated in the self.execute()
    // call, but the interpolation for the table and column names occur beforehand.
    parameters = (value, unique_id)
    try:
        logger.info("Updating", filtered_measure, "at", unique_id)
        self.cursor.execute(query, parameters)
        self.mariadb_connection.commit()
        logger.info("Updated", filtered_measure, "at", unique_id)
        if (self.cursor.rowcount != 1):
            raise KeyError
        return self.cursor.rowcount
    except pymysql.err.ProgrammingError as err:
        raise err
```

```
def _retrieve_by_timestamp_range(self, dstream, start, end):
    stringified_stream_token_uuid = _stringify_uuid(dstream["stream_token"])
    dstream_particulars = (stringified_stream_token_uuid, start, end)
    query = ("SELECT * FROM `%s` " % (stringified_stream_token_uuid)) + "WHERE time_stamp BETWEEN %s AND %s"
    #
    try:
        logger.info("Returning all records within timestamp range")
        self.cursor.execute(query, [start, end])
        # We have variables for start end, but the execute expects list or a tuple as its second argument,
        # so we place both in a list.
        results = self.cursor.fetchall()
        for row in results:
            logger.info(row)
        logger.info(self.cursor.rowcount)
        return self.cursor.rowcount
    except pymysql.err.ProgrammingError as err:
        raise err
```
