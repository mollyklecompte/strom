"""MySQL Database Connector Class"""

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"

#!/usr/bin/python
import copy
import mysql.connector as mariadb
from mysql.connector import errorcode
# relative path works when running mariadb.py as a module
from ..dstream.dstream import DStream
from ..dstream.filter_rules import FilterRules

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
            measure_columns += "  `" + measure + "` " + dstream['measures'][measure]['dtype'] + ","
            # measure_columns += "  `%s` %s,  " % (measure, dstream['measures'][measure]['dtype'])
        # print("***MEASURE COLUMNS****", measure_columns)

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            # uid dstream['user_ids'][uid]
            uid_columns += "  `" + uid + "` varchar(50),"
        # print("***UID COLUMNS***", uid_columns)

        filter_columns = ""
        # for each item in the filters dictionary
            # create a column for that filter
        for filt in dstream['filters']:
            # create a column filt for that filter
            # filter_columns += "  `" + filt["filter_name"] + "` varchar(50),"
            filter_columns += "  `" + filt["filter_name"] + "` varchar(20),"
            # print(filt)

        # parse stream_token to stringify and replace hyphens with underscores
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        # print("***STREAM TOKEN WITH UNDERSCORES***", stringified_stream_token_uuid)
        # stringified_stream_token_uuid = "stream_token_standin"

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
            ") ENGINE=InnoDB" % (stringified_stream_token_uuid, measure_columns, uid_columns, filter_columns))

        dstream_particulars = (measure_columns, uid_columns, filter_columns)

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

    def _insert_row_into_stream_lookup_table(self, dstream):
        # print("*** INPUT DSTREAM ***", dstream)
        # parse stream_token to stringify and replace hyphens with underscores
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        print("***STREAM TOKEN WITH UNDERSCORES***", stringified_stream_token_uuid)
        # stringified_stream_token_uuid = "stream_token_standin"

        measure_columns = ""
        # for each item in the measures dictionary
            # create a column for that measure
        for measure in dstream['measures']:
            # measure dstream['measures'][measure]['dtype']
            measure_columns += "  `" + measure + "`,"
            # measure_columns += "  `%s` %s,  " % (measure, dstream['measures'][measure]['dtype'])
        # print("***MEASURE COLUMNS****", measure_columns)

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            # uid dstream['user_ids'][uid]
            uid_columns += "  `" + uid + "`,"
        # print("***UID COLUMNS***", uid_columns)

        filter_columns = ""
        # for each item in the filters dictionary
            # create a column for that filter
        for filt in dstream['filters']:
            # create a column filt for that filter
            # filter_columns += "  `" + filt["filter_name"] + "` varchar(50),"
            filter_columns += "  `" + filt["filter_name"] + "`,"
            # print(filt)

        columns = (
            "(`version`,"
            " `time_stamp`,"
            "%s"
            "%s"
            "%s"
            " `tags`,"
            " `fields`)"
        % (measure_columns, uid_columns, filter_columns))

        measure_values = ""
        for key, value in dstream["measures"].items():
            measure_values += ' "' + str(value["val"]) + '",'

        uid_values = ""
        for key, value in dstream["user_ids"].items():
            uid_values += ' "' + str(value) + '",'

        filter_values = ""
        for filt in dstream['filters']:
            # create a column filt for that filter
            # filter_columns += "  `" + filt["filter_name"] + "` varchar(50),"
            filter_values += ' "' + str(filt["func_params"]) + '",'
            # print(filt)

        def _dictionary_to_string(dict):
            return '"' + str(dict) + '"'

        values = (
            "(%s, "
            "%s,"
            "%s"
            "%s"
            "%s"
            "%s,"
            "%s)"
        % (dstream["version"], dstream["timestamp"], measure_values, uid_values, filter_values, _dictionary_to_string(dstream["tags"]), _dictionary_to_string(dstream["fields"])))

        # print("****** COLUMNS ******", columns)
        # print("****** VALUES ******", values)

        query = ("INSERT INTO %s %s VALUES %s" % (stringified_stream_token_uuid, columns, values))
        # print("~~~~~~~~ QUERY ~~~~~~~~", query);

        try:
            print("Inserting row into table ", stringified_stream_token_uuid)
            self.cursor.execute(query)
            self.mariadb_connection.commit()
            print("Inserted row")
            # view data in cursor object
            # for (unique_id, version, tags, fields) in self.cursor:
            #     print("uid: {}, version: {}, tags: {}, fields: {}".format(unique_id, version, tags, fields))
            #     return [unique_id, version, tags, fields]
        except mariadb.Error as err:
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
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171117,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
}

second_single_dstream = {
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171118,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
}

third_single_dstream = {
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171119,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
}

fourth_single_dstream = {
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171120,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
}

fifth_single_dstream = {
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171121,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
}

sixth_single_dstream = {
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171122,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
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
#     'stream_token': UUID('2c16caea_caf4_11e7_ab05_0242eb7ab33c'),
#     'sources': {},
#     'measures': {},
#     'event_rules': {},
#     'dparam_rules': [],
#     'ingest_rules': {}
# }

# input_dstream = {
#     'foreign_keys': [],
#     'engine_rules': {},
#     'timestamp': 1510603538107,
#     'stream_token': UUID('61604bca-cbbe-11e7-ab05-0242eb7ab33c'),
#     'stream_name': 'driver_data',
#     'storage_rules': {},
#     'fields': {'region-code': 'PDX'},
#     'event_rules': {},
#     'tags': {},
#     'filters': [],
#     'version': 0,
#     'sources': {},
#     'user_ids': {'id': 0, 'driver-id': 'Molly Mora'},
#     'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'float'}},
#     'ingest_rules': {},
#     'dparam_rules': []
# }



def main():
    sql = SQL_Connection()
    # sql._create_metadata_table()
    # sql._insert_row("stream_one", 13, 1.0)
    # sql._insert_row("stream_two", 11, 1.1)
    # sql._retrieve_by_stream_name("stream_one")
    # sql._retrieve_by_id(1)
    # sql._retrieve_by_stream_token(11)
    # sql._select_all_from_metadata_table()



    dstream = DStream()
    # print("***DSTREAM INITIALIZED***:", dstream)

    second_row = copy.deepcopy(dstream)
    third_row = copy.deepcopy(dstream)
    fourth_row = copy.deepcopy(dstream)
    fifth_row = copy.deepcopy(dstream)

    print("dstream", dstream)
    print("SECOND ROW", second_row)

    # dstream._add_measure("m_2", "int(10)")
    # dstream._add_measure("m_1", "varchar(10)")
    # dstream._add_measure("m_3", "float(10, 2)")
    #
    # dstream._add_field("field_1")
    # dstream._add_field("field_2")
    # dstream._add_field("field_3")
    #
    # dstream._add_user_id("uid_1")
    # dstream._add_user_id("uid_2")
    # dstream._add_user_id("uid_3")
    #
    # dstream._add_tag("first tag")
    # dstream._add_tag("second tag")

    dstream.load_from_json(single_dstream)

    # filter_dict_1 = {"func_params":{}, "filter_name": "smoothing", "dtype":"float"}
    # filter_dict_2 = {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}
    #
    # dstream._add_filter({"func_params":{}, "filter_name": "smoothing", "dtype":"float"})
    # dstream._add_filter({"func_params":{}, "filter_name": "low_pass", "dtype":"float"})

    # print("DSTREAM WITH FILTERS ADDED", dstream)

    sql._create_stream_lookup_table(dstream)

    second_row.load_from_json(second_single_dstream)
    third_row.load_from_json(third_single_dstream)
    fourth_row.load_from_json(fourth_single_dstream)
    fifth_row.load_from_json(fifth_single_dstream)

    sql._insert_row_into_stream_lookup_table(dstream)
    # sql._insert_row_into_stream_lookup_table(dstream)
    # sql._insert_row_into_stream_lookup_table(dstream)

    sql._insert_row_into_stream_lookup_table(second_row)
    sql._insert_row_into_stream_lookup_table(third_row)
    sql._insert_row_into_stream_lookup_table(fourth_row)
    sql._insert_row_into_stream_lookup_table(fifth_row)

    # sql._retrieve_by_timestamp_range()

main()
