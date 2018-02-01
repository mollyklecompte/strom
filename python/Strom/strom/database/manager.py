""" Database Manager module for sqlite3, dataframes used for CRUD operations """
import pandas
import sqlite3 as sql
import pickle
import json

__author__= 'Adrian Agnic'
__version__= '0.0.1'


class Sql:

    __slots__=["db", "conn"]

    def __init__(self, filename):
        """
        :param filename: sqlite3 db file
        """
        self.db = str(filename)
        self.conn = None

    def _df_to_table(self, df, name):
        """ convert dataframe to sql table, replaces existing
        :param df: pandas dataframe
        :param name: name of table to create
        """
        df.to_sql(str(name), self.conn, if_exists="replace")

    def _data_to_table(self, data, column_names, table_name):
        """ create dataframe from given params & store as table
        :param data: 2-dimensional array of column data
        :param column_names: string array, declare the name of each column
        :param table_name: name of table to create
        """
        df = pandas.DataFrame(data, columns=column_names)
        self._df_to_table(df, str(table_name))

    def _query_df(self, statement, pars=None):
        """
        :param statement: string, sql query to execute
        :param pars: array of argmuents to pass
        :return: query result as dataframe
        """
        res = pandas.read_sql(str(statement), self.conn, params=pars)
        return res


    def connect(self):
        """ init db connection """
        self.conn = sql.connect(self.db)

    def close(self):
        """ stop db connection """
        self.conn.close()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def test(self):
        """ testing datatypes for possible errors """
        with open('dataframe.pkl', 'rb') as doc:
            df = pickle.load(doc)
            df["location"] = df["location"].apply(lambda x: json.dumps(x))
            df["not_location"] = df["not_location"].apply(lambda x: json.dumps(x))
            print(df)
            print(df, df.dtypes)
            self._df_to_table(df, "test")
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
