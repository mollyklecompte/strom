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

    # def test_create_metadata_table(self):
    #     self.cnx._create_metadata_table()

if __name__ == "__main__":
    unittest.main()
