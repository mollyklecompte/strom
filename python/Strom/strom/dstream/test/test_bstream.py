import unittest
from Strom.strom.dstream.bstream import BStream

class TestBStream(unittest.TestCase):
    def setUp(self):
        dstreams = [{"stream_name": "driver_data", "version": 0, "stream_token": None, "timestamp": 1510603538107, "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}, "measure2": {"val": 13, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}, {"stream_name": "driver_data", "version": 0, "stream_token": None, "timestamp": 1510603538108, "measures": {"location": {"val": [-132.69081962885704, 55.52110054870811], "dtype": "float"}, "measure2": {"val": 9, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}, {"stream_name": "driver_data", "version": 0, "stream_token": None, "timestamp": 1510603538109, "measures": {"location": {"val": [-142.69081962885704, 65.52110054870811], "dtype": "float"}, "measure2": {"val": 4, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}]
        template = {"stream_name": "driver_data", "version": 0, "stream_token": None, "timestamp": None, "measures": {"location": {"val": None, "dtype": "float"}, "measure2": {"val": None, "dtype": "int"}}, "fields": {"region-code": {}}, "user_ids": {"driver-id": {}, "id": {}}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "_id": "chadwick666"}
        self.bstream = BStream(template, dstreams)

    def test_init(self):
        self.assertEqual(self.bstream.template_id, "chadwick666")

    def test_load_from_dict(self):

        self.assertEqual(self.bstream["stream_name"], "driver_data")


    def test_aggregate_measures(self):
        self.bstream._aggregate_measures()



        self.assertEqual(self.bstream["measures"], {"location": {"val": [[-122.69081962885704, 45.52110054870811], [-132.69081962885704, 55.52110054870811], [-142.69081962885704, 65.52110054870811]], "dtype": "float"}, "measure2": {"val": [13, 9, 4], "dtype": "int"}})
        
    def test_aggregate_uids(self):
        self.bstream._aggregate_uids()
        
        self.assertEqual(self.bstream["user_ids"], {"driver-id": ["Molly Mora", "Molly Mora", "Molly Mora"], "id": [0, 0, 0]})
        
    def test_aggregate_ts(self):
        self.bstream._aggregate_ts()
        
        self.assertEqual(self.bstream["timestamp"], [1510603538107, 1510603538108, 1510603538109])
