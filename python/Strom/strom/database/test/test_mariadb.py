import unittest
from Strom.strom.database.mariadb import SQL_Connection

class TestSQL_Connection(unittest.TestCase):
    def setUp(self):
        self.cnx = SQL_Connection()
        self.cursor = self.cnx.cursor
        self.pool_name = self.cnx.pool_name

    def test_init(self):
        # cnx_pool_name = "my_pool"
        self.assertIsInstance(self.cnx, SQL_Connection)
        self.assertIs(self.cursor, self.cnx.cursor)
        self.assertEqual(self.pool_name, "my_pool")

    def test_create_metadata_table(self):
        self.assertIsNone(self.cnx._create_metadata_table())

    def test_insert_row(self):
        device_id = 10
        stream_token = 11
        version = 1.0
        self.cnx._insert_row(device_id, stream_token, version)
        self.assertEqual(self.cnx._retrieve_by_device_id(device_id), [1, 10, 11, 1.0])

    def test_retrieve_by_device_id(self):
        device_id = 11
        stream_token = 12
        version = 1.1
        self.cnx._insert_row(device_id, stream_token, version)
        self.assertEqual(self.cnx._retrieve_by_device_id(device_id), [2, 11, 12, 1.1])

    def test_retrieve_by_id(self):
        device_id = 12
        stream_token = 13
        version = 1.2
        self.cnx._insert_row(device_id, stream_token, version)
        self.assertEqual(self.cnx._retrieve_by_id(3), [3, 12, 13, 1.2])

    def test_select_all_from_metadata_table(self):
        self.assertIsNone(self.cnx._select_all_from_metadata_table())

if __name__ == "__main__":
    unittest.main()
