#!/usr/bin/python
import pymysql.cursors

dbconfig = {
    "user": 'user',
    "password": '123',
    "host": '172.17.0.3',
    "database": 'test',
    "charset": 'utf8mb4',
    "cursorclass": pymysql.cursors.DictCursor,
    "autocommit": True
}
