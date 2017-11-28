"""MySQL Database Connector Class"""

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"

#!/usr/bin/python
import copy
import gc
import logging

import pymysql.cursors
# from pymysql.err import pymysql.err.ProgrammingError, pymysql.err.InternalError
from pymysql.constants import ER
# relative path works when running mariadb.py as a module
from ..dstream.dstream import DStream
from ..dstream.filter_rules import FilterRules

def _stringify_by_adding_quotes(dict):
    return '"' + str(dict) + '"'

def _stringify_uuid(uuid):
    return str(uuid).replace("-", "_")

class SQL_Connection:
    def __init__(self):
        # Prevent connection leakage by manually invoking the Python garbage collector to avoid
        # running out of database connections.
        gc.collect()
        # Set up connection to 'test' database in the MariaDB instance on Docker
        # Implicit connection pool creation
        dbconfig = {
            "user": 'user',
            "password": '123',
            "host": '172.17.0.3',
            "database": 'test'
        }
        self.mariadb_connection = pymysql.connect(charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor, autocommit=True, **dbconfig)
        self.cursor = self.mariadb_connection.cursor()
        # self.mariadb_connection = mariadb.connect(pool_name = "my_pool", pool_size = 13, **dbconfig)
        # Create a cursor object to execute SQL commands
        # self.cursor = self.mariadb_connection.cursor(buffered=True)
        # self.pool_name = self.mariadb_connection.pool_name

    def _close_connection(self):
        # close pooled connection and return it to the connection pool as an available connection
        logging.info("Closing connection")
        self.mariadb_connection.close()
        gc.collect()

    # ***** Metadata Table and Methods *****

    def _create_metadata_table(self):
        table = ("CREATE TABLE template_metadata ("
            "  `unique_id` int(50) NOT NULL AUTO_INCREMENT,"
            "  `stream_name` varchar(60) NOT NULL,"
            "  `stream_token` varchar(60) NOT NULL,"
            "  `version` decimal(10, 2) NOT NULL,"
            "  `template_id` varchar(60) NOT NULL,"
            "  PRIMARY KEY (`unique_id`)"
            ") ENGINE=InnoDB")
        logging.info("Creating table")
        try:
            self.cursor.execute(table)
        except pymysql.err.InternalError as err:
            if ER.TABLE_EXISTS_ERROR:
                logging.error("table already exists")
            raise err

    def _insert_row_into_metadata_table(self, stream_name, stream_token, version, template_id):
        add_row = ("INSERT INTO template_metadata "
        "(stream_name, stream_token, version, template_id) "
        "VALUES (%s, %s, %s, %s)")
        stringified_stream_token_uuid = _stringify_uuid(stream_token)
        row_columns = (stream_name, stringified_stream_token_uuid, version, template_id)
        try:
            logging.info("Inserting row")
            self.cursor.execute(add_row, row_columns)
            self.mariadb_connection.commit()
            if (self.cursor.rowcount != 1):
                raise KeyError
            else:
                return self.cursor.rowcount
                logging.info("Row inserted")
        except pymysql.err.ProgrammingError as err:
            raise err

    def _retrieve_by_stream_name(self, stream_name):
        query = ('SELECT * FROM template_metadata WHERE stream_name = %s')
        try:
            logging.info("Querying by stream name")
            self.cursor.execute(query, [stream_name])
            results = self.cursor.fetchall()
            print("results for stream_name", results)
            for dictionary in results:
                logging.info("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(dictionary["unique_id"], dictionary["stream_name"], dictionary["stream_token"], dictionary["version"], dictionary["template_id"]))
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

    def _retrieve_by_id(self, unique_id):
        query = ("SELECT * FROM template_metadata WHERE unique_id = %s")
        try:
            logging.info("Querying by unique id")
            self.cursor.execute(query, [unique_id])
            result = self.cursor.fetchone()
            logging.info("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(result["unique_id"], result["stream_name"], result["stream_token"], result["version"], result["template_id"]))
            return result
        except pymysql.err.ProgrammingError as err:
            raise err

    def _retrieve_by_stream_token(self, stream_token):
        stringified_stream_token_uuid = _stringify_uuid(stream_token)
        query = ("SELECT * FROM template_metadata WHERE stream_token = %s")
        try:
            logging.info("Querying by stream token")
            self.cursor.execute(query, [stringified_stream_token_uuid])
            results = self.cursor.fetchall()
            for dictionary in results:
                logging.info("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(dictionary["unique_id"], dictionary["stream_name"], dictionary["stream_token"], dictionary["version"], dictionary["template_id"]))
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

    def _return_template_id_for_latest_version_of_stream(self, stream_token):
        stringified_stream_token_uuid = _stringify_uuid(stream_token)
        query = ("SELECT `template_id` FROM template_metadata WHERE stream_token = %s AND version = ("
                "SELECT MAX(version) FROM template_metadata WHERE stream_token = %s)")
        try:
            logging.info("Returning template_id for latest version of stream by stream_token")
            self.cursor.execute(query, [stringified_stream_token_uuid, stringified_stream_token_uuid])
            result = self.cursor.fetchall()
            print('template_id result', result)
            if len(result) == 1:
                logging.info(result[0]["template_id"])
                return result[0]["template_id"]
            else:
                raise pymysql.err.ProgrammingError
        except pymysql.err.ProgrammingError as err:
            raise err

    def _select_all_from_metadata_table(self):
        query = ("SELECT * FROM template_metadata")
        try:
            logging.info("Returning all data from template_metadata table")
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for row in results:
                logging.info(row)
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

    def _check_metadata_table_exists(self):
        query = ("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'template_metadata'")
        try:
            logging.info("Checking if template_metadata table exists")
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            print("results", results)
            print("results[0]['COUNT(*)']", results[0]['COUNT(*)'])
            if results[0]['COUNT(*)'] == 1:
                return True
            else:
                return False
        except pymysql.err.ProgrammingError as err:
            raise err

    def _check_table_exists(self, table_name):
        stringified_table_name = str(table_name).replace("-", "_")
        query = ("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test' AND table_name = %s")
        try:
            logging.info("Checking if table", stringified_table_name, "exists")
            self.cursor.execute(query, [stringified_table_name])
            results = self.cursor.fetchall()
            if results[0]['COUNT(*)'] == 1:
                return True
            else:
                return False
        except pymysql.err.ProgrammingError as err:
            raise err

# ***** Stream Token Table and Methods *****

    def _create_stream_lookup_table(self, dstream):

        measure_columns = ""
        # for each item in the measures dictionary
            # create a column for that measure
        for measure in dstream['measures']:
            measure_columns += "  `" + measure + "` " + dstream['measures'][measure]['dtype'] + ","

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            uid_columns += "  `" + uid + "` varchar(60),"

        filter_columns = ""
        # for each item in the filters dictionary
            # create a column for that filter
        for filt in dstream['filters']:
            filter_columns += "  `" + filt["filter_name"] + "` varchar(60),"

        stringified_stream_token_uuid = _stringify_uuid(dstream["stream_token"])

        table = ("CREATE TABLE %s ("
            "  `unique_id` int(10) NOT NULL AUTO_INCREMENT,"
            "  `version` decimal(10, 2) NOT NULL,"
            "  `time_stamp` decimal(20, 5) NOT NULL,"
            "%s"
            "%s"
            "%s"
            "  `tags` varchar(60),"
            "  `fields` varchar(60),"
            "  PRIMARY KEY (`unique_id`)"
            ") ENGINE=InnoDB" % (stringified_stream_token_uuid, measure_columns, uid_columns, filter_columns))

        dstream_particulars = (measure_columns, uid_columns, filter_columns)
        try:
            logging.info("Creating table")
            self.cursor.execute(table)
        except pymysql.err.ProgrammingError as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error("table already exists")
            else:
                raise err

    def _insert_row_into_stream_lookup_table(self, dstream):
        stringified_stream_token_uuid = _stringify_uuid(dstream["stream_token"])

        measure_columns = ""
        # for each item in the measures dictionary
            # create a column for that measure
        for measure in dstream['measures']:
            measure_columns += "  `" + measure + "`,"

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            uid_columns += "  `" + uid + "`,"

        columns = (
            "(`version`,"
            " `time_stamp`,"
            "%s"
            "%s"
            " `tags`,"
            " `fields`)"
        % (measure_columns, uid_columns))

        measure_values = ""
        for key, value in dstream["measures"].items():
            # measure_values += ' "' + str(value["val"]) + '",'
            measure_values += _stringify_by_adding_quotes(value["val"]) + ','

        uid_values = ""
        for key, value in dstream["user_ids"].items():
            # uid_values += ' "' + str(value) + '",'
            uid_values += _stringify_by_adding_quotes(value) + ','

        values = (
            "(%s, "
            "%s,"
            "%s"
            "%s"
            "%s,"
            "%s)"
        % (dstream["version"], dstream["timestamp"], measure_values, uid_values, _stringify_by_adding_quotes(dstream["tags"]), _stringify_by_adding_quotes(dstream["fields"])))

        # print("****** COLUMNS ******", columns)
        # print("****** VALUES ******", values)

        query = ("INSERT INTO %s %s VALUES %s" % (stringified_stream_token_uuid, columns, values))
        # print("~~~~~~~~ QUERY ~~~~~~~~", query);
        try:
            logging.info("Inserting row into table ", stringified_stream_token_uuid)
            self.cursor.execute(query)
            self.mariadb_connection.commit()
            logging.info("Inserted row")
            logging.info(self.cursor.lastrowid)
            return self.cursor.lastrowid
        except pymysql.err.ProgrammingError as err:
            raise err

    def _insert_filtered_measure_into_stream_lookup_table(self, stream_token, filtered_measure, value, unique_id):
        stringified_stream_token_uuid = _stringify_uuid(stream_token)
        query = ("UPDATE %s SET %s " % (stringified_stream_token_uuid, filtered_measure)) + "= %s WHERE unique_id = %s"
        parameters = (value, unique_id)
        try:
            logging.info("Updating", filtered_measure, "at", unique_id)
            self.cursor.execute(query, parameters)
            self.mariadb_connection.commit()
            logging.info("Updated", filtered_measure, "at", unique_id)
            if (self.cursor.rowcount != 1):
                raise KeyError
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

    def _retrieve_by_timestamp_range(self, dstream, start, end):
        stringified_stream_token_uuid = _stringify_uuid(dstream["stream_token"])
        dstream_particulars = (stringified_stream_token_uuid, start, end)
        query = ("SELECT * FROM %s " % (stringified_stream_token_uuid)) + "WHERE time_stamp BETWEEN %s AND %s"
        try:
            logging.info("Returning all records within timestamp range")
            self.cursor.execute(query, [start, end])
            results = self.cursor.fetchall()
            for row in results:
                logging.info(row)
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

    def _select_all_from_stream_lookup_table(self, dstream):
        stringified_stream_token_uuid = _stringify_uuid(dstream["stream_token"])
        query = ("SELECT * FROM %s" % stringified_stream_token_uuid)
        try:
            logging.info("Returning all records from stream lookup table ", stringified_stream_token_uuid)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for row in results:
                logging.info(row)
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

    def _select_data_by_column_where(self, dstream, data_column, filter_column, value):
        # Method created for testing purposes. Not intended for use by the coordinator (for now).
        stringified_stream_token_uuid = _stringify_uuid(dstream["stream_token"])
        query = ("SELECT %s FROM %s WHERE %s = %s" % (data_column, stringified_stream_token_uuid, filter_column, value))
        try:
            logging.info("Returning data")
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            logging.info(results)
            return self.cursor.rowcount
        except pymysql.err.ProgrammingError as err:
            raise err

single_dstream = {
    'stream_name': 'driver_data',
    'version': 0,
    'stream_token': 'test_token',
    'timestamp': 20171117,
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(60)'}},
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
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(60)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Kelson Agnic', 'id': 0},
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
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(60)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'David Parvizi', 'id': 0},
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
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(60)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Justine LeCompte', 'id': 0},
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
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(60)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Adrian Wang', 'id': 0},
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
    'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(60)'}},
    'fields': {'region-code': 'PDX'},
    'user_ids': {'driver-id': 'Parham Nielsen', 'id': 0},
    'tags': {},
    'foreign_keys': [],
    'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
    'dparam_rules': [],
    'event_rules': {}
}

def main():
    sql = SQL_Connection()
    sql._create_metadata_table()
    sql._check_metadata_table_exists()
    sql._check_table_exists('template_metadata')
    sql._insert_row_into_metadata_table("stream_one", "stream_token_one", 1.0, "temp_id_one")
    # sql._insert_row_into_metadata_table("stream_one", "stream_token_one", 1.0, "temp_id_one")
    sql._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.1, "temp_id_two")
    sql._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.2, "temp_id_three")
    # sql._insert_row_into_metadata_table("stream_one", "stream_token_one", 1.0, "filler")
    # sql._insert_row_into_metadata_table("stream_one", "stream_token_one", 1.0, "filler")
    # sql._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.1, "filler")
    # sql._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.2, "filler")
    # sql._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.2, "filler")
    # sql._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.2, "filler")
    sql._retrieve_by_stream_name("stream_one")
    sql._retrieve_by_id(1)
    # print("RETRIEVE ONE stream_two ROW")
    sql._retrieve_by_stream_token("stream_token_two")
    sql._return_template_id_for_latest_version_of_stream("stream_token_two")
    sql._select_all_from_metadata_table()



# STREAM LOOKUP TABLE PRELIMINARY TESTS

    dstream = DStream()
    # print("***DSTREAM INITIALIZED***:", dstream)

    dstream["stream_token"] = "gosh_darn"

    second_row = copy.deepcopy(dstream)
    third_row = copy.deepcopy(dstream)
    fourth_row = copy.deepcopy(dstream)
    fifth_row = copy.deepcopy(dstream)


    dstream.load_from_json(single_dstream)

    # print("@@@@ DSTREAM WITH DATA @@@@", dstream)

    sql._create_stream_lookup_table(dstream)
    sql._check_table_exists('gosh_darn')

    second_row.load_from_json(second_single_dstream)
    # print("@@@@ DSTREAM WITH second_single_dstream @@@@", second_row)
    third_row.load_from_json(third_single_dstream)
    # print("@@@@ DSTREAM WITH third_single_dstream @@@@", third_row)
    fourth_row.load_from_json(fourth_single_dstream)
    # print("@@@@ DSTREAM WITH fourth_single_dstream @@@@", fourth_row)
    fifth_row.load_from_json(fifth_single_dstream)
    # print("@@@@ DSTREAM WITH fifth_single_dstream @@@@", fifth_row)

    sql._insert_row_into_stream_lookup_table(dstream)


    sql._insert_row_into_stream_lookup_table(second_row)
    sql._insert_row_into_stream_lookup_table(third_row)
    sql._insert_row_into_stream_lookup_table(fourth_row)
    sql._insert_row_into_stream_lookup_table(fifth_row)

    # stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
    sql._insert_filtered_measure_into_stream_lookup_table(dstream["stream_token"], 'smoothing', 'dummy_data sldkfj lksjf lsajdlfj sl', 1)
    sql._insert_filtered_measure_into_stream_lookup_table(dstream["stream_token"], 'smoothing', 'test data sdfadsfafwt ergreag erg ', 2)
    sql._insert_filtered_measure_into_stream_lookup_table(dstream["stream_token"], 'smoothing', 'dummy data asdga ergawe gedawe erag', 3)
    sql._retrieve_by_timestamp_range(dstream, 20171117, 20171119)
    sql._select_all_from_stream_lookup_table(dstream)
    sql._select_data_by_column_where(dstream, "`driver-id`", "unique_id", 3)

    gc.collect()
    sql._close_connection()

# main()
