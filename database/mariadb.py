#!/usr/bin/python
import mysql.connector as mariadb
from mysql.connector import errorcode

# def _connect_to_database():
# Set up connection to 'test' database in the MariaDB instance on Docker
mariadb_connection = mariadb.connect(user='root', password='123', host='172.17.0.2', database='test')
# Create a cursor object to execute SQL commands
cursor = mariadb_connection.cursor()

def _create_metadata_table():
    table = ("CREATE TABLE template_metadata ("
        "  `unique_id` int(10) NOT NULL AUTO_INCREMENT,"
        "  `device_id` int(10) NOT NULL,"
        "  `stream_token` int(10) NOT NULL,"
        "  `version` float(10, 2) NOT NULL,"
        "  PRIMARY KEY (`unique_id`)"
        ") ENGINE=InnoDB")
    try:
        print("Creating table")
        cursor.execute(table)
    except mariadb.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists")
        else:
            print(err.msg)
    else:
        print("OK")

def _insert_row(device_id, stream_token, version):
    # There doesn't seem to be a way to use parameter placeholders for
    # table names; it can only be used to insert column values.
    add_row = ("INSERT INTO template_metadata "
    "(device_id, stream_token, version) "
    "VALUES (%s, %s, %s)")

    row_columns = (device_id, stream_token, version)

    try:
        print("Inserting row")
        cursor.execute(add_row, row_columns)
        mariadb_connection.commit()
        print("Row inserted")
    except mariadb.Error as err:
        print(err.msg)
    else:
        print("OK")

def _retrieve_by_device_id(device_id):
    query = ('SELECT * FROM template_metadata WHERE device_id = %s')
    try:
        print("Querying by device id")
        cursor.execute(query, [device_id])
        # view data in cursor object
        for (unique_id, device_id, stream_token, version) in cursor:
            print("uid: {}, device: {}, stream: {}, version: {}".format(unique_id, device_id, stream_token, version))
    except mariadb.Error as err:
        print(err.msg)
    else:
        print("OK")

def _retrieve_by_id(unique_id):
    query = ("SELECT * FROM template_metadata WHERE unique_id = %s")
    try:
        print("Querying by unique id")
        cursor.execute(query, [unique_id])
        # view data in cursor object
        for (unique_id, device_id, stream_token, version) in cursor:
            print("uid: {}, device: {}, stream: {}, version: {}".format(unique_id, device_id, stream_token, version))
    except mariadb.Error as err:
        print(err.msg)
    else:
        print("OK")

def _retrieve_by_stream_token(stream_token):
    query = ("SELECT * FROM template_metadata WHERE stream_token = %s")
    try:
        print("Querying by stream token")
        cursor.execute(query, [stream_token])
        # view data in cursor object
        for (unique_id, device_id, stream_token, version) in cursor:
            print("uid: {}, device: {}, stream: {}, version: {}".format(unique_id, device_id, stream_token, version))
    except mariadb.Error as err:
        print(err.msg)
    else:
        print("OK")

def main():
    # _connect_to_database()
    # _create_metadata_table()
    # _insert_row(12, 13, 1.0)
    # _insert_row(10, 11, 1.1)
    _retrieve_by_device_id(12)
    _retrieve_by_id(1)
    _retrieve_by_stream_token(11)
main()
