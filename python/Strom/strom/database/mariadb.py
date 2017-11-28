"""MySQL Database Connector Class"""
"""REFACTOR NEEDED FOR MODULARITY AND CODE REUSABILITY"""

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"

#!/usr/bin/python
import copy
import gc
import logging
import mysql.connector as mariadb
from mysql.connector import errorcode
# relative path works when running mariadb.py as a module
from ..dstream.dstream import DStream
from ..dstream.filter_rules import FilterRules

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
        self.mariadb_connection = mariadb.connect(pool_name = "my_pool", pool_size = 13, **dbconfig)
        # self.mariadb_connection = mariadb.connect(user='root', password='123', host='172.17.0.2', database='test')
        # Create a cursor object to execute SQL commands
        self.cursor = self.mariadb_connection.cursor(buffered=True)
        self.pool_name = self.mariadb_connection.pool_name

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
        except mariadb.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error("table already exists")
            raise err

    def _insert_row_into_metadata_table(self, stream_name, stream_token, version, template_id):
        add_row = ("INSERT INTO template_metadata "
        "(stream_name, stream_token, version, template_id) "
        "VALUES (%s, %s, %s, %s)")
        stringified_stream_token_uuid = str(stream_token).replace("-", "_")
        row_columns = (stream_name, stringified_stream_token_uuid, version, template_id)
        try:
            logging.info("Inserting row")
            self.cursor.execute(add_row, row_columns)
            self.mariadb_connection.commit()
            # rowcount property should == 1 for this single row insert method
            if (self.cursor.rowcount != 1):
                raise KeyError
            else:
                return self.cursor.rowcount
                logging.info("Row inserted")
        except mariadb.Error as err:
            raise err

    def _retrieve_by_stream_name(self, stream_name):
        query = ('SELECT * FROM template_metadata WHERE stream_name = %s')
        try:
            logging.info("Querying by stream name")
            self.cursor.execute(query, [stream_name])
            # for (unique_id, stream_name, stream_token, version, template_id) in self.cursor:
            #     print("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(unique_id, stream_name, stream_token, version, template_id))
            #     return [unique_id, stream_name, stream_token, float(version), template_id]
            results = self.cursor.fetchall()
            for row in results:
                # print(row)
                logging.info("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(row[0], row[1], row[2], row[3], row[4]))
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _retrieve_by_id(self, unique_id):
        query = ("SELECT * FROM template_metadata WHERE unique_id = %s")
        try:
            logging.info("Querying by unique id")
            self.cursor.execute(query, [unique_id])
            for (unique_id, stream_name, stream_token, version, template_id) in self.cursor:
                logging.info("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(unique_id, stream_name, stream_token, version, template_id))
                return [unique_id, stream_name, stream_token, float(version), template_id]
            # return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _retrieve_by_stream_token(self, stream_token):
        stringified_stream_token_uuid = str(stream_token).replace("-", "_")
        query = ("SELECT * FROM template_metadata WHERE stream_token = %s")
        try:
            logging.info("Querying by stream token")
            self.cursor.execute(query, [stringified_stream_token_uuid])
            for (unique_id, stream_name, stream_token, version, template_id) in self.cursor:
                logging.info("uid: {}, name: {}, stream: {}, version: {}, template_id: {}".format(unique_id, stream_name, stream_token, version, template_id))
                # return [unique_id, stream_name, stream_token, float(version), template_id]
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _return_template_id_for_latest_version_of_stream(self, stream_token):
        stringified_stream_token_uuid = str(stream_token).replace("-", "_")
        query = ("SELECT `template_id` FROM template_metadata WHERE stream_token = %s AND version = ("
                "SELECT MAX(version) FROM template_metadata WHERE stream_token = %s)")
        try:
            logging.info("Returning template_id for latest version of stream by stream_token")
            self.cursor.execute(query, [stringified_stream_token_uuid, stringified_stream_token_uuid])
            # for (template_id) in self.cursor:
            #     logging.info("template_id: {}".format(template_id))
            #     return template_id
            result = self.cursor.fetchall()
            if len(result) == 1:
                logging.info(result[0][0])
                return result[0][0]
            else:
                raise mariadb.Error
        except mariadb.Error as err:
            raise err

    def _select_all_from_metadata_table(self):
        query = ("SELECT * FROM template_metadata")
        try:
            logging.info("Returning all data from template_metadata table")
            self.cursor.execute(query)
            # for (unique_id, stream_name, stream_token, version, template_id, derived_id, events_id) in self.cursor:
            #     logging.info("uid: {}, name: {}, stream: {}, version: {}, template id: {}, derived id: {}, events id: {}".format(unique_id, stream_name, stream_token, version, template_id, derived_id, events_id))
            #     return [unique_id, stream_name, stream_token, version, template_id, derived_id, events_id]
            results = self.cursor.fetchall()
            for row in results:
                logging.info(row)
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _check_metadata_table_exists(self):
        query = ("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test' AND table_name = 'template_metadata'")
        try:
            logging.info("Checking if template_metadata table exists")
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            # logging.info(results[0][0])
            if results[0][0] == 1:
                return True
            else:
                return False
        except mariadb.Error as err:
            raise err

    def _check_table_exists(self, table_name):
        stringified_table_name = str(table_name).replace("-", "_")
        query = ("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'test' AND table_name = %s")
        try:
            logging.info("Checking if table", stringified_table_name, "exists")
            self.cursor.execute(query, [stringified_table_name])
            results = self.cursor.fetchall()
            # logging.info(results[0][0])
            if results[0][0] == 1:
                return True
            else:
                return False
        except mariadb.Error as err:
            raise err

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
        # logging.info("***MEASURE COLUMNS****", measure_columns)

        uid_columns = ""
        # for each item in the uids dictionary
            # create a column for that uid
        for uid in dstream['user_ids']:
            # uid dstream['user_ids'][uid]
            uid_columns += "  `" + uid + "` varchar(60),"
        # print("***UID COLUMNS***", uid_columns)

        filter_columns = ""
        # for each item in the filters dictionary
            # create a column for that filter
        for filt in dstream['filters']:
            # create a column filt for that filter
            # filter_columns += "  `" + filt["filter_name"] + "` varchar(50),"
            filter_columns += "  `" + filt["filter_name"] + "` varchar(60),"
            # print(filt)

        # parse stream_token to stringify and replace hyphens with underscores
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        # print("***STREAM TOKEN WITH UNDERSCORES***", stringified_stream_token_uuid)
        # stringified_stream_token_uuid = "stream_token_standin"

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
        except mariadb.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                logging.error("table already exists")
            else:
                raise err

    def _insert_row_into_stream_lookup_table(self, dstream):
        # print("*** INPUT DSTREAM ***", dstream)
        # parse stream_token to stringify and replace hyphens with underscores
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        # print("***STREAM TOKEN WITH UNDERSCORES***", stringified_stream_token_uuid)

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
            measure_values += ' "' + str(value["val"]) + '",'

        uid_values = ""
        for key, value in dstream["user_ids"].items():
            uid_values += ' "' + str(value) + '",'

        def _dictionary_to_string(dict):
            return '"' + str(dict) + '"'

        values = (
            "(%s, "
            "%s,"
            "%s"
            "%s"
            "%s,"
            "%s)"
        % (dstream["version"], dstream["timestamp"], measure_values, uid_values, _dictionary_to_string(dstream["tags"]), _dictionary_to_string(dstream["fields"])))

        # print("****** COLUMNS ******", columns)
        # print("****** VALUES ******", values)

        query = ("INSERT INTO %s %s VALUES %s" % (stringified_stream_token_uuid, columns, values))
        # print("~~~~~~~~ QUERY ~~~~~~~~", query);
        try:
            logging.info("Inserting row into table ", stringified_stream_token_uuid)
            self.cursor.execute(query)
            self.mariadb_connection.commit()
            logging.info("Inserted row")
            # for (unique_id, version, tags, fields) in self.cursor:
            #     logging.info("uid: {}, version: {}, tags: {}, fields: {}".format(unique_id, version, tags, fields))
            #     return [unique_id, version, tags, fields]
            logging.info(self.cursor.lastrowid)
            return self.cursor.lastrowid
        except mariadb.Error as err:
            raise err

    def _insert_filtered_measure_into_stream_lookup_table(self, stream_token, filtered_measure, value, unique_id):
        stringified_stream_token_uuid = str(stream_token).replace("-", "_")
        query = ("UPDATE %s SET %s " % (stringified_stream_token_uuid, filtered_measure)) + "= %s WHERE unique_id = %s"
        # print(len(value))
        parameters = (value, unique_id)
        try:
            logging.info("Updating", filtered_measure, "at", unique_id)
            self.cursor.execute(query, parameters)
            self.mariadb_connection.commit()
            # for (stream_token, filtered_measure, value, version, unique_id) in self.cursor:
            #     logging.info("Inserted {} into column {} where unique_id = {} into table {}".format(value, filtered_measure, unique_id, stream_token))
            #     logging.info("IN FOR LOOP")
            #     return [stream_token, filtered_measure, value, unique_id]
            logging.info("Executed", self.cursor.statement)
            if (self.cursor.rowcount != 1):
                raise KeyError
            # return self.cursor.statement
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _retrieve_by_timestamp_range(self, dstream, start, end):
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        dstream_particulars = (stringified_stream_token_uuid, start, end)
        query = ("SELECT * FROM %s WHERE time_stamp BETWEEN %s AND %s" % (stringified_stream_token_uuid, start, end))
        # query = ("SELECT * FROM %s WHERE time_stamp >= %s AND time_stamp <= %s" % (stringified_stream_token_uuid, start, end))
        # print("~~~~~~~~ QUERY ~~~~~~~~", query);
        try:
            logging.info("Returning all records within timestamp range")
            # self.cursor.execute(query, dstream_particulars)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for row in results:
                logging.info(row)
            # return results
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _select_all_from_stream_lookup_table(self, dstream):
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        query = ("SELECT * FROM %s" % stringified_stream_token_uuid)
        # logging.info("~~~~~~~~ QUERY ~~~~~~~~", query);
        try:
            logging.info("Returning all records from stream lookup table ", stringified_stream_token_uuid)
            # self.cursor.execute(query, dstream_particulars)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            for row in results:
                logging.info(row)
            # return results
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err

    def _select_data_by_column_where(self, dstream, data_column, filter_column, value):
        # Method created for testing purposes. Not intended for use by the coordinator (for now).
        stringified_stream_token_uuid = str(dstream["stream_token"]).replace("-", "_")
        query = ("SELECT %s FROM %s WHERE %s = %s" % (data_column, stringified_stream_token_uuid, filter_column, value))
        # logging.info("~~~~~~~~ QUERY ~~~~~~~~", query);
        try:
            logging.info("Returning data")
            # self.cursor.execute(query, dstream_particulars)
            self.cursor.execute(query)
            results = self.cursor.fetchall()
            # for row in results:
            #     logging.info(row)
            logging.info(results)
            # return results
            return self.cursor.rowcount
        except mariadb.Error as err:
            raise err
