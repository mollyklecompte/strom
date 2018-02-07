""" sub-class of pandadb, utilizes sqlite3 """
from strom.database.pandadb import PandaDB

__version__='0.0.1'
__author__='Adrian Agnic'

class SqliteDb(PandaDB):
    def __init__(self):
        super().__init__()

    def select(self):
        super().select("SELECT * FROM test;")

    def create(self):
        super().create()

    def read(self):
        super().read()

    def update(self):
        super().update()

    def delete(self):
        super().delete()

    def connect(self):
        super().connect()

    def close(self):
        super().close()
