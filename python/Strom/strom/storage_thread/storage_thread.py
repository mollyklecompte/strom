"""
StorageThread class

"""
from strom.database.mongo_management import MongoManager
from strom.database.maria_management import SQL_Connection
import threading

__version__ = "0.1"
__author__ = "Jessica <jessica@tura.io>"


class StorageRawThread(threading.Thread):
    def __init__(self, bstream):
        super().__init__()
        self._bstream = bstream
        self.maria = SQL_Connection()
        self.rows_inserted = None

    def run(self):
        self.rows_inserted = self.maria._insert_rows_into_stream_lookup_table(self._bstream)

class StorageFilteredThread(threading.Thread):
    def __init__(self, bstream):
        super().__init__()
        self._bstream = bstream
        self.maria = SQL_Connection()
        self.rows_inserted = None

    def run(self):
        filtered_dict = {"stream_token": self._bstream["stream_token"], "timestamp": self._bstream["timestamp"],
                         "filter_measures": self._bstream["filter_measures"]}
        self.rows_inserted = self.maria._insert_rows_into_stream_filtered_table(filtered_dict)

class StorageJsonThread(threading.Thread):
    def __init__(self, data, data_type):
        super().__init__()
        self._data = data
        self._data_type = data_type
        self.mongo = MongoManager()
        self.insert_id = None

    def run(self):
        self.insert_id = self.mongo.insert(self._data, self._data_type)
