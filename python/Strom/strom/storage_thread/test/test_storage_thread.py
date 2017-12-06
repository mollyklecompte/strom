import unittest
import json
from strom.database.maria_management import SQL_Connection
from strom.storage_thread.storage_thread import StorageRawThread
from strom.storage_thread.storage_thread import StorageFilteredThread
from strom.storage_thread.storage_thread import StorageJsonThread
from strom.database.mongo_management import MongoManager


class TestStorageThread(unittest.TestCase):
    def setUp(self):
        #maria setup
        self.cnx = SQL_Connection()
        self.cursor = self.cnx.cursor
        demo_data_dir = "./demo_data/"
        self.dstream = json.load(open(demo_data_dir + "demo_template.txt"))
        self.dstreams = json.load(open(demo_data_dir + "first_seven_from_demo_trip.txt"))
        self.bstream = json.load(open(demo_data_dir + "demo_bstream_trip26.txt"))

        #mongo setup
        self.mongo = MongoManager()

    def test_init(self):
        self.assertIsInstance(self.cnx, SQL_Connection)
        self.assertIs(self.cursor, self.cnx.cursor)

    def test_mariadb(self):
        # Initialize tables
        metadata_table = self.cnx._create_metadata_table()
        stream_lookup_table = self.cnx._create_stream_lookup_table(self.dstream)
        stream_filter_table = self.cnx._create_stream_filtered_table(self.dstream)

        # Check if tables exist
        self.assertTrue(self.cnx._check_metadata_table_exists())
        self.assertTrue(self.cnx._check_table_exists('template_metadata'))
        self.assertTrue(self.cnx._check_table_exists('abc123'))
        self.assertTrue(self.cnx._check_table_exists('abc123_filter'))

        # Insert rows into metadata_table
        self.assertEqual(self.cnx._insert_row_into_metadata_table("stream_one", "stream_token_one", 1.0, "filler"), 1)
        self.assertEqual(self.cnx._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.1, "woo"), 1)
        self.assertEqual(self.cnx._insert_row_into_metadata_table("stream_three", "stream_token_three", 1.2, "great"), 1)
        self.assertEqual(self.cnx._insert_row_into_metadata_table("stream_three", "stream_token_three", 1.3, "right"), 1)

        # Retrieve from metadata_table (TEST)
        self.assertDictEqual(self.cnx._retrieve_by_id(2), {"unique_id":2, "stream_name":"stream_two", "stream_token":"stream_token_two", "version":1.10, "template_id":"woo"})
        self.assertEqual(self.cnx._retrieve_by_stream_name("stream_two"), 1)
        self.assertEqual(self.cnx._retrieve_by_stream_token("stream_token_one"), 1)
        self.assertEqual(self.cnx._return_template_id_for_latest_version_of_stream("stream_token_three"), "right")
        self.assertEqual(self.cnx._select_all_from_metadata_table(), 4)

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

        inserted_id = self.mongo.insert(self.dstream, 'template')
        queried = self.mongo.get_by_id(inserted_id, 'template')

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

        self.assertEqual(inserted_id, queried["_id"])
        self.assertEqual(inserted_id2, queried2["_id"])
        self.assertEqual(inserted_id3, queried3["_id"])

    def test_store_json_thread_get_all_coll(self):
        self.mongo.insert(self.bstream, 'derived')
        self.mongo.insert(self.bstream, 'derived')

        test_thread_derived0 = StorageJsonThread(self.bstream,'derived')
        test_thread_derived1 = StorageJsonThread(self.bstream,'derived')
        test_thread_derived0.start()
        test_thread_derived0.join()
        test_thread_derived1.start()
        test_thread_derived1.join()
        test_thread_event0 = StorageJsonThread(self.bstream,'event')
        test_thread_event1 = StorageJsonThread(self.bstream,'event')
        test_thread_event0.start()
        test_thread_event0.join()
        test_thread_event1.start()
        test_thread_event1.join()

        all_derived = self.mongo.get_all_coll('derived','abc123')
        all_events = self.mongo.get_all_coll('event','abc123')

        self.assertTrue(len(all_derived) >= 2)
        self.assertTrue(len(all_events) >= 2)
        self.assertIn(self.mongo.derived_coll_suffix, all_derived[-1].keys())
        self.assertIn(self.mongo.event_coll_suffix, all_events[-1].keys())
        self.assertNotIn(self.mongo.event_coll_suffix, all_derived[-1].keys())
        self.assertNotIn(self.mongo.derived_coll_suffix, all_events[-1].keys())



    def tearDown(self):
        #close connections
        self.cnx._close_connection()





if __name__ == '__main__':
    unittest.main()
