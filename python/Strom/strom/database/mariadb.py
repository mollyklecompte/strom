"""MySQL Database Connector Class"""

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"

#!/usr/bin/python
import mysql.connector as mariadb
from mysql.connector import errorcode
# relative path works when running mariadb.py as a module
from ..dstream.dstream import DStream

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
            measure_columns += "  `" + measure + "` " + dstream['measures'][measure]['dtype'] + " NOT NULL,"

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            # uid dstream['user_ids'][uid]
            uid_columns += "  `" + uid + "` varchar(50) NOT NULL,"

        filter_columns = ""
        # for each item in the filters dictionary
            # create a column for that filter
        for filt in dstream['filters']:
            # create a column filt for that filter
            filter_columns += "  `" + filt + "` varchar(50) NOT NULL,"

        table = ("CREATE TABLE %s ("
            "  `unique_id` int(10) NOT NULL AUTO_INCREMENT,"
            "  `version` float(10, 2) NOT NULL,"
            "  `time_stamp` float(10, 2) NOT NULL,"
            "%s"
            "%s"
            "%s"
            "  `tags` varchar(50),"
            "  `fields` varchar(50),"
            "  PRIMARY KEY (`unique_id`)"
            ") ENGINE=InnoDB")

        dstream_particulars = (dstream['stream_token'], measure_columns, uid_columns, filter_columns)

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

    def _retrieve_by_timestamp_range(self, dstream, start, end):
        query = ("SELECT * FROM %s WHERE time_stamp BETWEEN %s AND %s")
        dstream_particulars = (dstream['stream_token'], start, end)
        try:
            print("Returning all records within timestamp range")
            self.cursor.execute(query, dstream_particulars)
            # view data in cursor object
            for (unique_id, version, tags, fields) in self.cursor:
                print("uid: {}, version: {}, tags: {}, fields: {}".format(unique_id, version, tags, fields))
                return [unique_id, version, tags, fields]
        except mariadb.Error as err:
            print(err.msg)
        else:
            print("OK")


single_dstream = {
    "stream_name": "driver_data",
    "version": 0,
    "stream_token": "test_token",
    "timestamp": 1510603538107,
    "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}},
    "fields": {"region-code": "PDX"},
    "user_ids": {"driver-id": "Molly Mora", "id": 0},
    "tags": {},
    "foreign_keys": [],
    "filters": [],
    "dparam_rules": [],
    "event_rules": {}
}

# initial_dstream = {
#     'storage_rules': {},
#     'tags': [],
#     'filters': [],
#     'timestamp': None,
#     'engine_rules': {},
#     'user_ids': {},
#     'foreign_keys': {},
#     'device_id': None,
#     'version': 0,
#     'fields': {},
#     'stream_token': UUID('2c16caea-caf4-11e7-ab05-0242eb7ab33c'),
#     'sources': {},
#     'measures': {},
#     'event_rules': {},
#     'dparam_rules': [],
#     'ingest_rules': {}
# }

def main():
    sql = SQL_Connection()
    print(sql.pool_name)
    #  sql._create_metadata_table()
    #  sql._insert_row("stream_one", 13, 1.0)
    #  sql._insert_row("stream_two", 11, 1.1)
    #  sql._retrieve_by_stream_name("stream_one")
    #  sql._retrieve_by_id(1)
    #  sql._retrieve_by_stream_token(11)
    #  sql._select_all_from_metadata_table()

    dstream = DStream()
    print("***DSTREAM INITIALIZED***:", dstream)

        #  def _add_measure(self, measure_name, dtype):
        #      """Creates entry in measures dict for new measure"""
        #      self["measures"][measure_name] = {"val":None, "dtype":dtype}

    dstream._add_measure("m_2", "int(10)")
    dstream._add_measure("m_1", "varchar(10)")
    dstream._add_measure("m_3", "float(10, 2)")
    #
    #
    #     #  def _add_field(self, field_name):
    #     #      self["fields"][field_name] = {}
    #
    dstream._add_field("field_1")
    dstream._add_field("field_2")
    dstream._add_field("field_3")
    #
    #
    #     #  def _add_user_id(self, id_name):
    #     #      self["user_ids"][id_name] = {}
    #
    dstream._add_user_id("uid_1")
    dstream._add_user_id("uid_2")
    dstream._add_user_id("uid_3")
    #
    #
    #     #  def _add_tag(self, tag):
    #     #      self["tags"].append(tag)
    #
    dstream._add_tag("first tag")
    dstream._add_tag("second tag")
    #
    #
    #     #  def _add_filter(self, filter_dict):
    #     #      """Add filter to our storage.
    #     #       filter_dict: dict of parameters for filter class object"""
    #     #      self["filters"].append(filter_dict)
    #
    filter_dict = {
        "filter_1": "first filter",
        "filter_2": "second filter",
        "filter_3": "third filter"
    }
    dstream._add_filter(filter_dict)
    #
    #     #  def _publish_version(self):
    #     #      """Increment version number"""
    #     #      self["version"] += 1
    #
    #
    print("***DSTREAM WITH LOOKUP TABLE FIELDS***:", dstream)

main()
