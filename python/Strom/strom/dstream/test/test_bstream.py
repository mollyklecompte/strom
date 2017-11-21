import unittest
import json
from Strom.strom.dstream.bstream import DStream, BStream

class TestBStream(unittest.TestCase):
    def setUp(self):
        dstreams = [{"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": 1510603538107, "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}, "measure2": {"val": 13, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "chill"}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}, {"stream_name": "driver_data", "version": 0, "stream_token": "abc123","sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {},"timestamp": 1510603538108, "measures": {"location": {"val": [-132.69081962885704, 55.52110054870811], "dtype": "float"}, "measure2": {"val": 9, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "big mood"}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}, {"stream_name": "driver_data", "version": 0, "stream_token": "abc123","sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {},"timestamp": 1510603538109, "measures": {"location": {"val": [-142.69081962885704, 65.52110054870811], "dtype": "float"}, "measure2": {"val": 4, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "the last big mood"}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}]
        template = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": None, "measures": {"location": {"val": None, "dtype": "float"}, "measure2": {"val": None, "dtype": "int"}}, "fields": {"region-code": None}, "user_ids": {"driver-id": {}, "id": {}}, "tags": {"mood": None}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "_id": "chadwick666"}
        ids = [1,2,3]
        self.bstream = BStream(template, dstreams, ids)

    def test_init(self):
        self.assertEqual(self.bstream["template_id"], "chadwick666")
        self.assertEqual(self.bstream.ids, [1,2,3])

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

    def test_aggregate_fields(self):
        self.bstream._aggregate_fields()

        self.assertEqual(self.bstream["fields"], {"region-code": ["PDX", "PDX", "PDX"]})

    def test_aggregate_tags(self):
        self.bstream._aggregate_tags()

        self.assertEqual(self.bstream["tags"], {"mood": ["chill", "big mood", "the last big mood"]})

    def test_aggregate(self):
        b = self.bstream.aggregate()
        x = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {},'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": [1510603538107, 1510603538108, 1510603538109], "measures":{"location": {"val": [[-122.69081962885704, 45.52110054870811], [-132.69081962885704, 55.52110054870811], [-142.69081962885704, 65.52110054870811]], "dtype": "float"}, "measure2": {"val": [13, 9, 4], "dtype": "int"}}, "fields": {"region-code": ["PDX", "PDX", "PDX"]}, "user_ids": {"driver-id": ["Molly Mora", "Molly Mora", "Molly Mora"], "id": [0, 0, 0]}, "tags": {"mood": ["chill", "big mood", "the last big mood"]}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "template_id": "chadwick666"}

        print("B STREAM")
        print(b)
        print("TEST ONE")
        print(x)

        self.assertEqual(b,x)

    def test_applying(self):
        dtemplate = DStream()
        ids = [1, 2, 3]
        dtemplate.load_from_json({"_id": "fromMONGO","stream_name": "driver_data", "version": 0, "stream_token": "token_stream", "timestamp": None,
                                  "measures": {"location": {"val": None, "dtype": "float"}}, "fields": {"region-code": {}},
                                  "user_ids": {"driver-id": {}, "id": {}}, "tags": {}, "foreign_keys": [],
                                  "filters": [], "dparam_rules": [], "event_rules": {}})
        first_filter_rule = {}
        first_filter_rule["func_type"] = "filter_data"
        first_filter_rule["func_name"] = "ButterLowpass"
        first_filter_rule["filter_name"] = "buttery_location"
        first_filter_rule["func_params"] = {"order":2, "nyquist":0.05}
        first_filter_rule["measures"] = ["location"]

        second_filter_rule = {}
        second_filter_rule["func_type"] = "filter_data"
        second_filter_rule["func_name"] = "WindowAverage"
        second_filter_rule["filter_name"] = "window_location"
        second_filter_rule["func_params"] = {"window_len":3}
        second_filter_rule["measures"] = ["location"]

        heading_params = {}
        heading_params["filter_name"] = "bears"
        heading_params["func_name"] = "DeriveHeading"
        heading_params["func_type"] = "derive_param"
        heading_params["func_params"] = {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}
        heading_params["measure_rules"] = {"spatial_measure": "location", "output_name": "bears"}
        heading_params["measures"] = ["location"]

        event_params = {}
        event_params["func_type"] = "detect_event"
        event_params["func_name"] = "DetectThreshold"
        event_params["event_rules"] = {"measure": "bears", "threshold_value": 90, "comparison_operator": ">="}
        event_params["event_name"] = "turn90"
        event_params["stream_token"] = dtemplate["stream_token"]
        event_params["measures"] = ["location"]
        event_params["filter_measurs"] = ["window_location"]
        event_params["derived_measures"] = ["bears"]


        bdict = json.load(open("Strom/strom/transform/bstream.txt"))
        bdict["timestamp"] = bdict["timestamp"][:100]
        bdict["measures"]["location"]["val"] = bdict["measures"]["location"]["val"][:100]
        for tag in bdict["tags"].keys():
            bdict["tags"][tag] = bdict["tags"][tag][:100]
        for uid in bdict["user_ids"].keys():
            bdict["user_ids"][uid] = bdict["user_ids"][uid][:100]

        bstream = BStream(dtemplate, {}, ids)
        bstream._load_from_dict(bdict)
        bstream["filters"].append(first_filter_rule)
        bstream["filters"].append(second_filter_rule)
        bstream["dparam_rules"].append(heading_params)
        bstream["event_rules"][event_params["event_name"]] = event_params
        bstream.apply_filters()
        self.assertIsInstance(bstream["filter_measures"], dict)
        self.assertIn(first_filter_rule["filter_name"], bstream["filter_measures"])
        self.assertIsInstance(bstream["filter_measures"][first_filter_rule["filter_name"]]["val"], list)

        bstream.apply_dparam_rules()
        self.assertIsInstance(bstream["derived_measures"], dict)
        self.assertIn(heading_params["measure_rules"]["output_name"], bstream["derived_measures"])
        self.assertIsInstance(bstream["derived_measures"][heading_params["measure_rules"]["output_name"]]["val"], list)

        bstream.find_events()
        self.assertIsInstance(bstream["events"], dict)
        self.assertIn(event_params["event_name"], bstream["events"])
        self.assertIsInstance(bstream["events"][event_params["event_name"]], list)
        self.assertIsInstance(bstream["events"][event_params["event_name"]][0], dict)


