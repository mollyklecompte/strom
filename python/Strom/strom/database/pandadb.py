""" abstract database class w/ default basic pandas sql statements"""
import pandas
from abc import ABCMeta, abstractmethod

__version__='0.0.1'
__author__='Adrian Agnic'


class PandaDB(metaclass=ABCMeta):

    def __init__(self, conn):
        self.conn = conn
        super().__init__()

    @abstractmethod
    def select(self, query, pars, table):
        """ standard sql parametrized select, default select all w/ given table name
        :type query: str
        :param query: standard sql query with ? or '?' for params
        :type pars: list, tuple
        :param pars: values to be passed into query
        :return: pandas
        """
        if not query:
            stmnt = "SELECT * FROM {0}".format(table)
            defres = pandas.read_sql(stmnt, self.conn)
            return defres
        res = pandas.read_sql(str(query), self.conn, params=pars)
        return res

    @abstractmethod
    def create(self, query, pars, df, table):
        """
        :type query: str
        :param query: standard sql query with ? or '?' for params
        :type pars: list, tuple
        :param pars: values to be passed into query
        :type df: pandas
        :param df: row(s), as a dataframe, to be created
        :type table: str
        :param table: name of table
        """
        if not df:
            pandas.read_sql(str(query), self.conn, params=pars)
        dfres = self.select(table='yup')
        dfcat = dfres.concat(df, ignore_index=True)
        dfcat.to_sql(str(table), self.conn, if_exists="replace")

    @abstractmethod
    def update(self, query, pars, df, table):
        """
        :type query: str
        :param query: standard sql query with ? or '?' for params
        :type pars: list, tuple
        :param pars: values to be passed into query
        :type df: pandas
        :param df: row, as a dataframe, to be updated
        :type table: str
        :param table: name of table
        """
        if not df:
            pandas.read_sql(str(query), self.conn, params=pars)
        dfres = self.select(table='yup')
        dfres.loc[df['index'], :] = df
        dfres.to_sql(str(table), self.conn, if_exists="replace")

    @abstractmethod
    def delete(self, query, pars):
        """ standard sql parametrized delete
        :type query: str
        :param query: standard sql query with ? or '?' for params
        :type pars: list, tuple
        :param pars: values to be passed into query
        :return: pandas
        """
        pandas.read_sql(str(query), self.conn, params=pars)

    @abstractmethod
    def table(self, df, table):
        """
        :type df: pandas
        :param df: dataframe to seed table with
        :type table: str
        :param table: name of table
        """
        df.to_sql(str(table), self.conn, if_exists="replace")

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        self.conn.close()
