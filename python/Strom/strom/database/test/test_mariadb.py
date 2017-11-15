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
        stream_name = "stream_one"
        stream_token = 11
        version = 1.0
        self.cnx._insert_row(stream_name, stream_token, version)
        self.assertEqual(self.cnx._retrieve_by_stream_name(stream_name), [1, "stream_one", 11, 1.0])

    def test_retrieve_by_id(self):
        stream_name = "stream_two"
        stream_token = 12
        version = 1.1
        self.cnx._insert_row(stream_name, stream_token, version)
        self.assertEqual(self.cnx._retrieve_by_id(2), [2, "stream_two", 12, 1.1])

    def test_retrieve_by_stream_name(self):
        stream_name = "stream_three"
        stream_token = 13
        version = 1.2
        self.cnx._insert_row(stream_name, stream_token, version)
        self.assertEqual(self.cnx._retrieve_by_stream_name(stream_name), [3, "stream_three", 13, 1.2])

    def test_retrieve_by_stream_token(self):
        self.assertEqual(self.cnx._retrieve_by_stream_token(13), [3, "stream_three", 13, 1.2])

    def test_select_all_from_metadata_table(self):
        self.assertIsNone(self.cnx._select_all_from_metadata_table())

    def test_close_connection(self):
        self.assertIsNone(self.cnx._close_connection())

if __name__ == "__main__":
    unittest.main()
