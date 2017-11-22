import unittest
import gc
from strom.database.mariadb import SQL_Connection
from strom.dstream.dstream import DStream


class TestSQL_Connection(unittest.TestCase):
    def setUp(self):
        self.cnx = SQL_Connection()
        self.cursor = self.cnx.cursor
        self.pool_name = self.cnx.pool_name

    def test_init(self):
        self.assertIsInstance(self.cnx, SQL_Connection)
        self.assertIs(self.cursor, self.cnx.cursor)
        self.assertEqual(self.pool_name, "my_pool")

    def test_mariadb(self):

        # Set up dummy dstreams
        single_dstream = {
            'stream_name': 'driver_data',
            'version': 0,
            'stream_token': 'test_mariadb_stream_lookup_table',
            'timestamp': 20171117,
            'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
            'fields': {'region-code': 'PDX'},
            'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
            'tags': {},
            'foreign_keys': [],
            'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
            'dparam_rules': [],
            'event_rules': {}
        }

        second_single_dstream = {
            'stream_name': 'driver_data',
            'version': 0,
            'stream_token': 'test_mariadb_stream_lookup_table',
            'timestamp': 20171118,
            'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
            'fields': {'region-code': 'PDX'},
            'user_ids': {'driver-id': 'Kelson Agnic', 'id': 0},
            'tags': {},
            'foreign_keys': [],
            'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
            'dparam_rules': [],
            'event_rules': {}
        }

        third_single_dstream = {
            'stream_name': 'driver_data',
            'version': 0,
            'stream_token': 'test_mariadb_stream_lookup_table',
            'timestamp': 20171119,
            'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
            'fields': {'region-code': 'PDX'},
            'user_ids': {'driver-id': 'David Parvizi', 'id': 0},
            'tags': {},
            'foreign_keys': [],
            'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
            'dparam_rules': [],
            'event_rules': {}
        }

        fourth_single_dstream = {
            'stream_name': 'driver_data',
            'version': 0,
            'stream_token': 'test_mariadb_stream_lookup_table',
            'timestamp': 20171120,
            'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
            'fields': {'region-code': 'PDX'},
            'user_ids': {'driver-id': 'Justine LeCompte', 'id': 0},
            'tags': {},
            'foreign_keys': [],
            'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
            'dparam_rules': [],
            'event_rules': {}
        }

        fifth_single_dstream = {
            'stream_name': 'driver_data',
            'version': 0,
            'stream_token': 'test_mariadb_stream_lookup_table',
            'timestamp': 20171121,
            'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
            'fields': {'region-code': 'PDX'},
            'user_ids': {'driver-id': 'Adrian Wang', 'id': 0},
            'tags': {},
            'foreign_keys': [],
            'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
            'dparam_rules': [],
            'event_rules': {}
        }

        sixth_single_dstream = {
            'stream_name': 'driver_data',
            'version': 0,
            'stream_token': 'test_mariadb_stream_lookup_table',
            'timestamp': 20171122,
            'measures': {'location': {'val': [-122.69081962885704, 45.52110054870811], 'dtype': 'varchar(50)'}},
            'fields': {'region-code': 'PDX'},
            'user_ids': {'driver-id': 'Parham Nielsen', 'id': 0},
            'tags': {},
            'foreign_keys': [],
            'filters': [{"func_params":{}, "filter_name": "smoothing", "dtype":"float"}, {"func_params":{}, "filter_name": "low_pass", "dtype":"float"}],
            'dparam_rules': [],
            'event_rules': {}
        }

        # Initialize tables
        metadata_table = self.cnx._create_metadata_table()
        stream_lookup_table = self.cnx._create_stream_lookup_table(single_dstream)

        # Check if metadata_table exists
        self.assertTrue(self.cnx._check_metadata_table_exists())

        # Insert rows into metadata_table
        self.cnx._insert_row_into_metadata_table("stream_one", "stream_token_one", 1.0, "filler")
        self.cnx._insert_row_into_metadata_table("stream_two", "stream_token_two", 1.1, "filler")
        self.cnx._insert_row_into_metadata_table("stream_three", "stream_token_three", 1.2, "filler")

        # Retrieve from metadata_table (TEST)
        self.assertEqual(self.cnx._retrieve_by_id(2), [2, "stream_two", "stream_token_two", 1.1, "filler"])
        self.assertEqual(self.cnx._retrieve_by_stream_name("stream_three"), [3, "stream_three", "stream_token_three", 1.2, "filler"])
        self.assertEqual(self.cnx._retrieve_by_stream_token("stream_token_three"), [3, "stream_three", "stream_token_three", 1.2, "filler"])
        self.assertIsNone(self.cnx._select_all_from_metadata_table())

        # Insert rows into stream_lookup_table
        self.cnx._insert_row_into_stream_lookup_table(single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(second_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(third_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(fourth_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(fifth_single_dstream)

        # Insert value into filtered measure in stream_lookup_table
        # self.cnx._insert_filtered_measure_into_stream_lookup_table('test_mariadb_stream_lookup_table', 'smoothing', 'dummy data', 1)
        self.assertEqual(self.cnx._insert_filtered_measure_into_stream_lookup_table('test_mariadb_stream_lookup_table', 'smoothing', 'dummy data', 1), "UPDATE test_mariadb_stream_lookup_table SET smoothing = 'dummy data' WHERE unique_id = 1")

        # Retrieve from stream_lookup_table
        self.assertTrue(self.cnx._retrieve_by_timestamp_range(single_dstream, 20171117, 20171118))
        self.assertEqual(self.cnx._return_template_id_for_latest_version_of_stream("stream_token_three"), "filler")

        # Close connection
        gc.collect()
        self.cnx._close_connection()



if __name__ == "__main__":
    unittest.main()
