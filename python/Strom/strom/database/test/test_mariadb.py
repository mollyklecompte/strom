import unittest
from Strom.strom.database.mariadb import SQL_Connection
from Strom.strom.dstream.dstream import DStream

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
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
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
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
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
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
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
    'user_ids': {'driver-id': 'Molly Mora', 'id': 0},
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

    def test_insert_row_into_metadata_table(self):
        stream_name = "stream_one"
        stream_token = 11
        version = 1.0
        id_filler = "filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, id_filler, id_filler, id_filler)
        self.assertEqual(self.cnx._retrieve_by_stream_name(stream_name), [1, "stream_one", 11, 1.0, "filler", "filler", "filler"])

    # def test_insert_row_into_stream_lookup_table(self):
    #     self.cnx._insert_row_into_stream_lookup_table(single_dstream)
    #     self.assertEqual(self.cnx._retrieve_by_timestamp_range('test_mariadb_stream_lookup_table', 20171117, 20171118), )

    def test_retrieve_by_id(self):
        stream_name = "stream_two"
        stream_token = 12
        version = 1.1
        id_filler = "filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, id_filler, id_filler, id_filler)
        self.assertEqual(self.cnx._retrieve_by_id(2), [2, "stream_two", 12, 1.1, "filler", "filler", "filler"])

    def test_retrieve_by_stream_name(self):
        stream_name = "stream_three"
        stream_token = 13
        version = 1.2
        id_filler = "filler"
        self.cnx._insert_row_into_metadata_table(stream_name, stream_token, version, id_filler, id_filler, id_filler)
        self.assertEqual(self.cnx._retrieve_by_stream_name(stream_name), [3, "stream_three", 13, 1.2, "filler", "filler", "filler"])

    def test_retrieve_by_stream_token(self):
        self.assertEqual(self.cnx._retrieve_by_stream_token(13), [3, "stream_three", 13, 1.2, "filler", "filler", "filler"])

    # def test_retrieve_by_timestamp_range(self):

    def test_select_all_from_metadata_table(self):
        self.assertIsNone(self.cnx._select_all_from_metadata_table())

if __name__ == "__main__":
    unittest.main()
