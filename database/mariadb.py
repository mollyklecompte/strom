#!/usr/bin/python
import mysql.connector as mariadb

mariadb_connection = mariadb.connect(user='root', password='123', host='172.17.0.2', database='test')
cursor = mariadb_connection.cursor()
