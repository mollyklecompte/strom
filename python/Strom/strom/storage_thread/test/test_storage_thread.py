import unittest
from strom.storage_thread.storage_thread import StorageRawThread
from strom.storage_thread.storage_thread import StorageFilteredThread
from strom.storage_thread.storage_thread import StorageJsonThread


class TestStorageThread(unittest.TestCase):
    def setup(self):
        #test bstream
        #self.test_bstream
        # molly suggest running coordinater.process_template first to setup tables.
        self.storage_raw_thread = StorageRawThread()
        self.storage_filtered_thread = StorageFilteredThread()
        self.storage_json_thread = StorageJsonThread()

    def test_store_raw_thread(self):
        test_rows_inserted = self.storage_raw_thread(self.test_bstream).start()
        self.assertEqual()



if __name__ == '__main__':
    unittest.main()
