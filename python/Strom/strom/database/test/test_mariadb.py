import unittest
from strom.database.mariadb import SQL_Connection
from strom.dstream.dstream import DStream

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

    # def test_close_connection(self):
    #     self.assertIsNone(self.cnx._close_connection())

    def test_create_metadata_table(self):
        self.assertIsNone(self.cnx._create_metadata_table())

    def test_create_stream_lookup_table(self):
        # self.dstream = DStream()
        self.assertIsNone(self.cnx._create_stream_lookup_table(single_dstream))

    def test_insert_filtered_measure_into_stream_lookup_table(self):
        stream_token = "test_mariadb_stream_lookup_table"
        filtered_measure = "smoothing"
        value = "dummy data for filtered_measure"
        unique_id = 2
        self.cnx._insert_filtered_measure_into_stream_lookup_table(stream_token, filtered_measure, value, unique_id)

    def test_insert_row_into_metadata_table(self):
        stream_name = "stream_one"
        stream_token = "stream_token_one"
        version = 1.0
        template_id = "filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, template_id)
        self.assertEqual(self.cnx._retrieve_by_stream_name(stream_name), [1, "stream_one", "stream_token_one", 1.0, "filler"])

    def test_insert_row_into_stream_lookup_table(self):
        self.cnx._insert_row_into_stream_lookup_table(single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(second_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(third_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(fourth_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(fifth_single_dstream)
        # self.assertEqual(self.cnx._retrieve_by_timestamp_range('test_mariadb_stream_lookup_table', 20171117, 20171118), )
        self.assertEqual(self.cnx._select_data_by_column_where(single_dstream, "`driver-id`", "unique_id", 3), [('David Parvizi',)])

    def test_retrieve_by_id(self):
        stream_name = "stream_two"
        stream_token = "stream_token_two"
        version = 1.1
        template_id = "filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, template_id)
        self.assertEqual(self.cnx._retrieve_by_id(2), [2, "stream_two", "stream_token_two", 1.1, "filler"])

    def test_retrieve_by_stream_name(self):
        stream_name = "stream_three"
        stream_token = "stream_token_three"
        version = 1.2
        template_id = "filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, template_id)
        self.assertEqual(self.cnx._retrieve_by_stream_name(stream_name), [3, "stream_three", "stream_token_three", 1.2, "filler"])

    def test_retrieve_by_stream_token(self):
        self.assertEqual(self.cnx._retrieve_by_stream_token("stream_token_three"), [3, "stream_three", "stream_token_three", 1.2, "filler"])

    def test_retrieve_by_timestamp_range(self):
        self.cnx._insert_row_into_stream_lookup_table(single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(second_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(third_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(fourth_single_dstream)
        self.cnx._insert_row_into_stream_lookup_table(fifth_single_dstream)
        self.assertTrue(self.cnx._retrieve_by_timestamp_range(single_dstream, 20171117, 20171118))

    def test_return_template_id_for_latest_version_of_stream(self):
        stream_name = "stream_four"
        stream_token = "stream_token_three"
        version = 1.3
        template_id = "not filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, template_id)
        self.assertEqual(self.cnx._return_template_id_for_latest_version_of_stream("stream_token_three"), template_id)

    def test_select_all_from_metadata_table(self):
        self.assertIsNone(self.cnx._select_all_from_metadata_table())

    def test_select_data_by_column_where(self):
        self.cnx._insert_row_into_stream_lookup_table(single_dstream)
        self.assertEqual(self.cnx._select_data_by_column_where(single_dstream, "`driver-id`", "unique_id", 1), [('Molly Mora',)])
        # close connect after last test
        self.cnx._close_connection()

if __name__ == "__main__":
    unittest.main()
