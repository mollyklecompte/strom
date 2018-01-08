"""

StorageThread module includes 3 thread classes, StorageRawThread, StorageFilteredThread,
and StorageJsonThread. The storage threads are called from the Coordinator.process_data_async to
kickoff bstream storage calls concurrently. The respective thread run() method is executed when
the thread is started in the Coordinator, ie. raw_thread.start()

"""
from strom.database.mongo_management import MongoManager
from strom.database.maria_management import SQL_Connection
from strom.utils.stopwatch import *

from logging import DEBUG
import threading
__version__ = "0.1"
__author__ = "Jessica <jessica@tura.io>"


class StorageRawThread(threading.Thread):
    """

        StorageRawThread created with bstream dict to store raw measures from bstream.
        Attributes:
          bstream:    the bstream from Coordinator
          maria:      SQL connection created.
          rows_inserted:  returned from Maria insert(for testing).

    """
    def __init__(self, bstream):
        super().__init__()
        self._bstream = bstream
        self.maria = SQL_Connection()
        self.rows_inserted = None
        stopwatch['raw_thread'].logger_level = DEBUG
        stopwatch['raw_thread'].start()

    def run(self):
        self.rows_inserted = self.maria._insert_rows_into_stream_lookup_table(self._bstream)
        stopwatch['raw_thread'].lap()

class StorageFilteredThread(threading.Thread):
    """

        StorageFilteredThread is created with bstream dict to store token, timestamp list,
        filtered measures to maria manager.
        Attributes:
          bstream:    the bstream from Coordinator
          maria:      SQL connection created.
          rows_inserted:  returned from Maria insert(for testing).

    """
    def __init__(self, bstream):
        super().__init__()
        self._bstream = bstream
        self.maria = SQL_Connection()
        self.rows_inserted = None
        stopwatch['filtered_thread'].logger_level = DEBUG
        stopwatch['filtered_thread'].start()

    def run(self):
        filtered_dict = {
                        "stream_token": self._bstream["stream_token"],
                        "timestamp": self._bstream["timestamp"],
                        "filter_measures": self._bstream["filter_measures"]
                        }
        self.rows_inserted = self.maria._insert_rows_into_stream_filtered_table(filtered_dict)
        stopwatch['filtered_thread'].lap()

class StorageJsonThread(threading.Thread):
    """

        StorageJsonThread is created with bstream dict and data_type string
        to store either derived params or events in Mongo.

        Attributes:
            data_type:  string 'derived' or 'event'
            data:       bstream dict
            mongo:      MongoManager instance
            insert_id:  returned from mongo insert

    """

    def __init__(self, data, data_type):
        super().__init__()
        self._data = data
        self._data_type = data_type
        self.mongo = MongoManager()
        self.insert_id = None
        stopwatch['mongo_thread'].logger_level = DEBUG
        stopwatch['mongo_thread'].start()

    def run(self):
        self.insert_id = self.mongo.insert(self._data, self._data_type)
        stopwatch['mongo_thread'].lap()
