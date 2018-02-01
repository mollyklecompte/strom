import pandas
import sqlite3 as sql

__author__= 'Adrian Agnic'
__version__= '0.0.1'


class Sql:

    __slots__=["db", "conn"]

    def __init__(self, filename):
        """
        :param filename: sqlite3 db file
        """
        self.db = filename

    def _df_to_table(self, df, name):
        """ convert dataframe to sql table
        :param df: pandas dataframe
        :param name: name of table to create
        """
        df.to_sql(name, self.conn, if_exists="replace")

    def _query(self, statement):
        """
        :param statement: string, sql query to execute
        :return: query result as dataframe
        """
        res = pandas.read_sql(statement, self.conn)
        return res


    def connect(self):
        """ init db connection """
        self.conn = sql.connect(self.db)

    def close(self):
        """ stop db connection """
        self.conn.close()
