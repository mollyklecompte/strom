import unittest
import json
from strom.database.maria_management import SQL_Connection
from strom.storage_thread.storage_thread import StorageRawThread
from strom.storage_thread.storage_thread import StorageFilteredThread
from strom.storage_thread.storage_thread import StorageJsonThread
from strom.database.mongo_management import MongoManager


demo_data_dir = "./demo_data/"
dstream = json.load(open(demo_data_dir + "demo_template.txt"))


class TestStorageThread(unittest.TestCase):
    def setUp(self):
        #maria setup
        self.cnx = SQL_Connection()
        self.cursor = self.cnx.cursor
        self.dstream = dstream
        self.dstreams = json.load(open(demo_data_dir + "first_seven_from_demo_trip.txt"))
        self.bstream = json.load(open(demo_data_dir + "demo_bstream_trip26.txt"))
        self.mongo = MongoManager()

    @classmethod
    def setUpClass(cls):
        cnx = SQL_Connection()
        cnx._create_metadata_table()
        cnx._create_stream_lookup_table(dstream)
        cnx._create_stream_filtered_table(dstream)

    def test_store_raw_thread(self):
        test_thread = StorageRawThread(self.bstream)
        test_thread.start()
        test_thread.join()
        self.assertEqual(test_thread.rows_inserted, 283)

    def test_store_filtered_thread(self):
        test_thread = StorageFilteredThread(self.bstream)
        test_thread.start()
        test_thread.join()
        self.assertEqual(test_thread.rows_inserted, 283)

    def test_store_json_thread_insert_retrieve(self):
        token = self.dstream["stream_token"]

        #inserted_id2 = self.mongo.insert(self.bstream, 'derived')
        test_thread_derived = StorageJsonThread(self.bstream,'derived')
        test_thread_derived.start()
        test_thread_derived.join()
        inserted_id2 = test_thread_derived.insert_id
        queried2 = self.mongo.get_by_id(inserted_id2, 'derived', token)

        #inserted_id3 = self.mongo.insert(self.bstream, 'event')
        test_thread_event = StorageJsonThread(self.bstream,'event')
        test_thread_event.start()
        test_thread_event.join()
        inserted_id3 = test_thread_event.insert_id
        queried3 = self.mongo.get_by_id(inserted_id3, 'event', token)

        self.assertEqual(inserted_id2, queried2["_id"])
        self.assertEqual(inserted_id3, queried3["_id"])

    def tearDown(self):
        #close connections
        self.cnx._close_connection()




if __name__ == '__main__':
    unittest.main()
