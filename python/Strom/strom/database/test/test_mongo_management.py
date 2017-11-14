import unittest
from Strom.strom.database.mongo_management import MongoManager


class TestMongoManager(unittest.TestCase):
    def setUp(self):
        self.manager = MongoManager()

    # def test_insert_template(self):