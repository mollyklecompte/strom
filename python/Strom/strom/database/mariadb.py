"""MySQL Database Connector Class"""

#!/usr/bin/python
import mysql.connector as mariadb
from mysql.connector import errorcode

class SQL_Connection:
    def __init__(self):
        # Set up connection to 'test' database in the MariaDB instance on Docker
        # Implicit connection pool creation
        dbconfig = {
            "user": 'root',
            "password": '123',
            "host": '172.17.0.2',
            "database": 'test'
        }
        self.mariadb_connection = mariadb.connect(pool_name = "my_pool", pool_size = 10, **dbconfig)
        # self.mariadb_connection = mariadb.connect(user='root', password='123', host='172.17.0.2', database='test')
        # Create a cursor object to execute SQL commands
        self.cursor = self.mariadb_connection.cursor()
        self.pool_name = self.mariadb_connection.pool_name

    def _close_connection(self):
        # close pooled connection and return it to the connection pool as an available connection
        print("Closing connection")
        self.mariadb_connection.close()

    # ***** Metadata Table and Methods *****

    def _create_metadata_table(self):
        table = ("CREATE TABLE template_metadata ("
            "  `unique_id` int(10) NOT NULL AUTO_INCREMENT,"
            "  `stream_name` varchar(20) NOT NULL,"
            "  `stream_token` int(10) NOT NULL,"
            "  `version` float(10, 2) NOT NULL,"
            "  `template_id` varchar(20),"
            "  `derived_id` varchar(30),"
            "  `events_id` varchar(30),"
            "  PRIMARY KEY (`unique_id`)"
            ") ENGINE=InnoDB")
        try:
            print("Creating table")
            self.cursor.execute(table)
        except mariadb.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists")
            else:
                print(err.msg)
        else:
            print("OK")

    def _insert_row(self, stream_name, stream_token, version):
        # There doesn't seem to be a way to use parameter placeholders for
        # table names; it can only be used to insert column values.
        add_row = ("INSERT INTO template_metadata "
        "(stream_name, stream_token, version) "
        "VALUES (%s, %s, %s)")

        row_columns = (stream_name, stream_token, version)

        try:
            print("Inserting row")
            self.cursor.execute(add_row, row_columns)
            self.mariadb_connection.commit()
            print("Row inserted")
        except mariadb.Error as err:
            print(err.msg)
        else:
            print("OK")

    def _retrieve_by_stream_name(self, stream_name):
        query = ('SELECT * FROM template_metadata WHERE stream_name = %s')
        try:
            print("Querying by stream name")
            self.cursor.execute(query, [stream_name])
            # view data in cursor object
            for (unique_id, stream_name, stream_token, version, template_id, derived_id, events_id) in self.cursor:
                print("uid: {}, name: {}, stream: {}, version: {}, template id: {}, derived id: {}, events id: {}".format(unique_id, stream_name, stream_token, version, template_id, derived_id, events_id))
                return [unique_id, stream_name, stream_token, version, template_id, derived_id, events_id]
        except mariadb.Error as err:
            print(err.msg)
        else:
            print("OK")

    def _retrieve_by_id(self, unique_id):
        query = ("SELECT * FROM template_metadata WHERE unique_id = %s")
        try:
            print("Querying by unique id")
            self.cursor.execute(query, [unique_id])
            # view data in cursor object
            for (unique_id, stream_name, stream_token, version, template_id, derived_id, events_id) in self.cursor:
                print("uid: {}, name: {}, stream: {}, version: {}, template id: {}, derived id: {}, events id: {}".format(unique_id, stream_name, stream_token, version, template_id, derived_id, events_id))
                return [unique_id, stream_name, stream_token, version, template_id, derived_id, events_id]
        except mariadb.Error as err:
            print(err.msg)
        else:
            print("OK")

    def _retrieve_by_stream_token(self, stream_token):
        query = ("SELECT * FROM template_metadata WHERE stream_token = %s")
        try:
            print("Querying by stream token")
            self.cursor.execute(query, [stream_token])
            # view data in cursor object
            for (unique_id, stream_name, stream_token, version, template_id, derived_id, events_id) in self.cursor:
                print("uid: {}, name: {}, stream: {}, version: {}, template id: {}, derived id: {}, events id: {}".format(unique_id, stream_name, stream_token, version, template_id, derived_id, events_id))
                return [unique_id, stream_name, stream_token, version, template_id, derived_id, events_id]
        except mariadb.Error as err:
            print(err.msg)
        else:
            print("OK")

    def _select_all_from_metadata_table(self):
        query = ("SELECT * FROM template_metadata")
        try:
            print("Returning all data from template_metadata table")
            self.cursor.execute(query)
            # view data in cursor object
            for (unique_id, stream_name, stream_token, version, template_id, derived_id, events_id) in self.cursor:
                print("uid: {}, name: {}, stream: {}, version: {}, template id: {}, derived id: {}, events id: {}".format(unique_id, stream_name, stream_token, version, template_id, derived_id, events_id))
        except mariadb.Error as err:
            print(err.msg)
        else:
            print("OK")


# ***** Stream Token Table and Methods *****

    def _create_stream_lookup_table(self, dstream):
        # take in a dstream python object

        measure_columns = ""
        # for each item in the measures dictionary
            # create a column for that measure
        for measure in dstream['measures']:
            # measure dstream['measures'][measure]['dtype']
            measure_columns += "  ` +" measure "` " + dstream['measures'][measure]['dtype'] + ","

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            # uid dstream['user_ids'][uid]

        # for each item in the filters dictionary
            # create a column for that filter
        for filt in dstream['filters']:
            # create a column filt for that filter

        table = ("CREATE TABLE %s ("
            "  `unique_id` int(10) NOT NULL AUTO_INCREMENT,"
            "  `version` float(10, 2) NOT NULL,"
            "  `time_stamp` date NOT NULL,"
            "  `filters` varchar(50),"
            "  `tags` varchar(50),"
            "  `fields` varchar(50),"
            "  PRIMARY KEY (`unique_id`)"
            ") ENGINE=InnoDB")

        dstream_particulars = (dstream['stream_token'])

        try:
            print("Creating table")
            self.cursor.execute(table, dstream_particulars)
        except mariadb.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists")
            else:
                print(err.msg)
        else:
            print("OK")

# def main():
#     #  _connect_to_database()
#     #  _create_metadata_table()
#     #  _insert_row("stream_one", 13, 1.0)
#     #  _insert_row("stream_two", 11, 1.1)
#     #  _retrieve_by_stream_name("stream_one")
#     #  _retrieve_by_id(1)
#     #  _retrieve_by_stream_token(11)
#
#      sql = SQL_Connection()
#      print(sql.pool_name)
#      sql._create_metadata_table()
#      sql._insert_row("stream_one", 13, 1.0)
#      sql._insert_row("stream_two", 11, 1.1)
#      sql._retrieve_by_stream_name("stream_one")
#      sql._retrieve_by_id(1)
#      sql._retrieve_by_stream_token(11)
#      sql._select_all_from_metadata_table()
# main()
