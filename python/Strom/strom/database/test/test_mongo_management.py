import unittest
from Strom.strom.database.mongo_management import MongoManager
from Strom.strom.dstream.dstream import DStream


class TestMongoManager(unittest.TestCase):
    def setUp(self):
        self.manager = MongoManager()
        self.dstream = DStream()
        self.dstream["device_id"] = 'Chad'
        self.dstream["stream_token"] = 'abc123'

    def test_insert(self):
        token = self.dstream["stream_token"]

        inserted_id = self.manager.insert(self.dstream, 'template')
        queried = self.manager.get_by_id(inserted_id, 'template')

        inserted_id2 = self.manager.insert(self.dstream, 'derived')
        queried2 = self.manager.get_by_id(inserted_id2, 'derived', token)

        inserted_id3 = self.manager.insert(self.dstream, 'event')
        queried3 = self.manager.get_by_id(inserted_id3, 'event', token)

        self.assertEqual("Chad", queried["device_id"])
        self.assertEqual("Chad", queried2["device_id"])
        self.assertEqual("Chad", queried3["device_id"])

        self.assertEqual(inserted_id, queried["_id"])
        self.assertEqual(inserted_id2, queried2["_id"])
        self.assertEqual(inserted_id3, queried3["_id"])


if __name__ == "__main__":
    unittest.main()