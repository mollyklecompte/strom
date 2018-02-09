""" sub-class of pandadb, utilizes sqlite3 """
import sqlite3 as sql
import pickle
from pandadb import PandaDB, serialize

__version__='0.0.1'
__author__='Adrian Agnic'


class SqliteDB(PandaDB):

    def __init__(self, filename):
        self.db = str(filename)
        self.conn = None

    def connect(self):
        """ pandadb override """
        self.conn = sql.connect(self.db)
        super().__init__(self.conn)

    def close(self):
        super().close()

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def select(self, query=None, pars=None, table=None):
        return super().select(query, pars, table)

    def create(self, query=None, pars=None, df=None, table=None):
        super().create(query, pars, df, table)

    def table(self, df, table, action="replace"):
        super().table(df, table, action)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    def table_exists(self, table):
        return super().table_exists(table)

    def retrieve(self):
        pass

    def test(self):
        with open('dataframe.pkl', 'rb') as doc:
            df = pickle.load(doc)
            df["location"] = df["location"].apply(lambda x: serialize(x))
            df["not_location"] = df["not_location"].apply(lambda x: serialize(x))
            self.table(df=df, table='test')
