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
        self.db = filename
        self.conn = None

    def _df_to_table(self, df, name):
        """ convert dataframe to sql table
        :param df: pandas dataframe
        :param name: name of table to create
        """
        df.to_sql(name, self.conn, if_exists="replace")

    def _data_to_table(self, data, column_names, table_name):
        """ create dataframe from given params & store as table
        :param data: 2-dimensional array of column data
        :param column_names: string array, declare the name of each column
        :param table_name: name of table to create
        """
        df = pandas.DataFrame(data, columns=column_names)
        self._df_to_table(df, table_name)

    def _query(self, statement):
        """
        :param statement: string, sql query to execute
        :return: query result as dataframe
        """
        res = pandas.read_sql(statement, self.conn)
        return res

    @staticmethod
    def _makequerystr(func, quant, this=None, that=None):
        return None


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

    def insert(self):
        pass
    def select(self):
        pass
    def update(self):
        pass
    def delete(self):
        pass
