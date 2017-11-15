import unittest
from Strom.strom.database.mongo_management import MongoManager
from Strom.strom.dstream.dstream import DStream


class TestMongoManager(unittest.TestCase):
    def setUp(self):
        self.manager = MongoManager()
        self.dstream = DStream()

    def test_insert_template(self):
        inserted_id = self.manager._insert_template(self.dstream)
        queried = self.manager._get_template(self.dstream['stream-token'])
        print(inserted_id)
        print(queried['stream_token'])

        self.assertEqual(inserted_id, queried['stream_token'])

    #def test_get_template(self):
     #   queried = self.manager._get_template(self.dstream['stream-token'])



if __name__ == "__main__":
    unittest.main()