""" abstract database class w/ default basic pandas sql statements
~ NOTES ~
*   pandas.to_sql method creates columns 'index' and 'level_0' for backup reference to original indices
*   contains static wrapper function around json.dumps if needed
*   more stable with dataframes passed instead of straight queries
~ ISSUES ~
*   problem with certain queries: delete, insert, drop, update REASON: read_sql() returning count instead of columns
    --FIX: use query() method
"""
import pandas
import json
from abc import ABCMeta, abstractmethod

__version__='0.0.1'
__author__='Adrian Agnic'


class PandaDB(metaclass=ABCMeta):

    def __init__(self, conn):
        """
        :type conn: database connection object
        :param conn: must assign this property in child class through connect() override
        """
        self.conn = conn
        super().__init__()

    @abstractmethod
    def connect(self):
        """ override with specific engine connection method """
        pass

    @abstractmethod
    def close(self):
        self.conn.close()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~BASIC METHODS
    @abstractmethod
    def select(self, query, pars, table):
        """ standard sql parametrized select, default select all if just given table name
        EX. result = db.select(table='test') or db.select(query="select * from test;")
        :type query: str
        :param query: standard sql query with ? or '?' for params
        :type pars: list, tuple
        :param pars: values to be passed into query
        :type table: str
        :param table: name of table
        :return: pandas
        """
        if not query:
            stmnt = "SELECT * FROM {0}".format(str(table))
            defres = pandas.read_sql(stmnt, self.conn)
            return defres
        res = pandas.read_sql(str(query), self.conn, params=pars)
        return res

    @abstractmethod
    def table(self, df, table, action):
        """
        EX. db.table(df=myDataframe, table='test', action='replace')
        :type df: pandas
        :param df: dataframe to seed table with
        :type table: str
        :param table: name of table
        :type action: str
        :param action: action to take if table exists: 'replace', 'append', or 'fail'
        """
        df.to_sql(str(table), self.conn, if_exists=str(action))

    @abstractmethod
    def create(self, query, pars, df, table):
        """ similar to table method but w/ query functionality and flexibility with # of columns
        EX. db.create(df=myDataframe, table='test')
        :type query: str
        :param query: standard sql query with ? or '?' for params
        :type pars: list, tuple
        :param pars: values to be passed into query
        :type df: pandas
        :param df: row(s), as a dataframe, to be created
        :type table: str
        :param table: name of table
        """
        if df is None:
            pandas.read_sql(str(query), self.conn, params=pars)
        try:
            df.to_sql(str(table), self.conn, if_exists="append")
        except:
            daf = self.select(table=str(table))
            daf = daf.append(df)
            daf.to_sql(str(table), self.conn, if_exists="replace")

    @abstractmethod
    def query(self, stmnt):
        """ normal database query method, override this"""
        pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~SPECIFIC METHODS
    @abstractmethod
    def exists(self, table):
        """
        :type table: str
        :param table: name of table
        :return: boolean
        """
        try:
            bewl = self.select(table=str(table))
        except Exception as err:
            print(err)
            return False
        return True

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~RETRIEVAL METHODS
    @abstractmethod
    def retrieve(self):
        pass

    #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
def serialize(stuff):
    return json.dumps(stuff)

# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~NOTE(s)
# # list of col names: df.columns.values.tolist()
# # ndf.index[-1] last index of DF
# # df.at[1, 'id'] = 20
# #~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
