import unittest
from Strom.strom.coordinator.coordinator import Coordinator

class TestCoordinator(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator()
        self.dstream_template = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "timestamp": None, "measures": {"location": {"val": None, "dtype": "float"}, "measure2": {"val": None, "dtype": "int"}}, "fields": {"region-code": {}}, "user_ids": {"driver-id": {}, "id": {}}, "tags": {"mood": None}, "foreign_keys": [], "filters": [ {"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}}
        self.queried_template = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "timestamp": None, "measures": {"location": {"val": None, "dtype": "float"}, "measure2": {"val": None, "dtype": "int"}}, "fields": {"region-code": {}}, "user_ids": {"driver-id": {}, "id": {}}, "tags": {"mood": None}, "foreign_keys": [], "filters": [ {"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}, "_id": "chadwick666"}
        self.dstream = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "timestamp": 1510603538107, "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {}, "foreign_keys": [], "filters": [ {"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}}
        self.dstreams = [{"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": 1510603538107, "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}, "measure2": {"val": 13, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "chill"}, "foreign_keys": [],"filters": [{"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}}, 
                         {"stream_name": "driver_data", "version": 0, "stream_token": "abc123","sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {},"timestamp": 1510603538108, "measures": {"location": {"val": [-132.69081962885704, 55.52110054870811], "dtype": "float"}, "measure2": {"val": 9, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "big mood"}, "foreign_keys": [], "filters": [{"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}}, 
                         {"stream_name": "driver_data", "version": 0, "stream_token": "abc123","sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {},"timestamp": 1510603538109, "measures": {"location": {"val": [-142.69081962885704, 65.52110054870811], "dtype": "float"}, "measure2": {"val": 4, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "the last big mood"}, "foreign_keys": [], "filters": [{"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}}]
        self.bstream = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {},'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": [1510603538107, 1510603538108, 1510603538109], "measures":{"location": {"val": [[-122.69081962885704, 45.52110054870811], [-132.69081962885704, 55.52110054870811], [-142.69081962885704, 65.52110054870811]], "dtype": "float"}, "measure2": {"val": [13, 9, 4], "dtype": "int"}}, "fields": {"region-code": ["PDX", "PDX", "PDX"]}, "user_ids": {"driver-id": ["Molly Mora", "Molly Mora", "Molly Mora"], "id": [0, 0, 0]}, "tags": {"mood": ["chill", "big mood", "the last big mood"]}, "foreign_keys": [],"filters": [{"func_type": "filter_data", "func_name": "WindowAverage", "filter_name": "window_location", "func_params": {"window_len": 2}, "measures": ["location"]}], "dparam_rules": [{"filter_name": "bears", "func_name": "DeriveHeading", "func_type": "derive_param", "func_params": {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat": True}, "measure_rules": {"spatial_measure": "location", "output_name": "bears"}, "measures": ["location"]}], "event_rules": {"ninety_degree_turn": {"func_type": "detect_event", "func_name": "DetectThreshold", "event_rules": {"measure": "change_in_heading", "threshold_value": 90, "comparison_operator": ">="}, "event_name": "ninety_degree_turn", "stream_token": None, "derived_measures": ["change_in_heading"]}}, "template_id": "chadwick666"}
       
    def test_store_json(self):
        inserted_template_id = self.coordinator._store_json(self.dstream_template, 'template')
        inserted_derived_id = self.coordinator._store_json(self.derived_bstream, 'derived')
        inserted_event_id = self.coordinator._store_json(self.events_bstream, 'event')

        queried_template = self.coordinator.mongo.get_by_id(inserted_template_id, 'template')
        queried_derived = self.coordinator.mongo.get_by_id(inserted_derived_id, 'derived', token=self.queried_template["stream_token"])
        queried_event= self.coordinator.mongo.get_by_id(inserted_event_id, 'event', token=self.queried_template["stream_token"])

        self.assertEqual(inserted_template_id, queried_template["_id"])
        self.assertEqual(inserted_derived_id, queried_derived["_id"])
        self.assertEqual(inserted_event_id, queried_event["_id"])

    def test_list_to_bstream(self):
        bstream = self.coordinator._list_to_bstream(self.queried_template, self.dstreams, [1,2,3])

        self.assertEqual(bstream, self.bstream)
        self.assertEqual(bstream.ids, [1,2,3])


    def test_retrieve_current_template(self):
        qt = self.coordinator._retrieve_current_template("abc123")
        self.assertEqual(qt, self.queried_template)

    def test_process_template(self):
        self.dstream_template["stream_token"] = "a_token_token"
        self.coordinator.process_template(self.dstream_template)
        qt = self.coordinator._retrieve_current_template(self.dstream_template["stream_token"])
        self.assertEqual(qt["stream_token"], self.dstream_template["stream_token"])
        self.assertEqual(qt["stream_name"], self.dstream_template["stream_name"])
        self.dstream_template["version"] = 1
        self.coordinator.process_template(self.dstream_template)
        qt = self.coordinator._retrieve_current_template(self.dstream_template["stream_token"])
        self.assertEqual(qt["version"], 1)

    def test_process_data_sync(self):
        self.coordinator.process_template(self.dstream_template)
        self.coordinator.process_data_sync(self.dstreams, self.dstream_template["stream_token"])


if __name__ == "__main__":
    unittest.main()