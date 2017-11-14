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

# TABLES['template-metadata'] = (
#     "CREATE TABLE `template-metadata` ("
#     "  `unique_id` int(10) NOT NULL AUTO_INCREMENT,"
#     "  `device_id` date NOT NULL,"
#     "  `version` int(10) NOT NULL,"
#     "  PRIMARY KEY (`unique_id`)"
#     ") ENGINE=InnoDB")

# # Create tables by iterating over all the items in the TABLES Python dictionary
# for name, ddl in TABLES.items():
#     try:
#         print("Creating table {}: ".format(name), end='')
#         cursor.execute(ddl)
#     except mariadb.Error as err:
#         if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
#             print("already exists.")
#         else:
#             print(err.msg)
#     else:
#         print("OK")

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

# # Test inserting a row into the employees table
# add_employee = ("INSERT INTO employees "
#                "(first_name, last_name, hire_date, gender, birth_date) "
#                "VALUES (%s, %s, %s, %s, %s)")
#
# tomorrow = datetime.now().date() + timedelta(days=1)
#
# data_employee = ('Geert', 'Vanderkelen', tomorrow, 'M', date(1977, 6, 14))
#
# cursor.execute(add_employee, data_employee)
#
# # Commit data to the database
# mariadb_connection.commit()

def main():
    # _connect_to_database()
    _create_metadata_table()
    _insert_row(12, 13, 1.0)
main()
