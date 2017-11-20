import unittest
from Strom.strom.coordinator.coordinator import Coordinator

class TestCoordinator(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator()
        self.dstream_template = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "timestamp": None, "measures": {"location": {"val": None, "dtype": "float"}}, "fields": {"region-code": {}}, "user_ids": {"driver-id": {}, "id": {}}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}
        self.queried_template = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "timestamp": None, "measures": {"location": {"val": None, "dtype": "float"}}, "fields": {"region-code": {}}, "user_ids": {"driver-id": {}, "id": {}}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "_id": "chadwick666"}
        self.dstream = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "timestamp": 1510603538107, "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}
        self.dstreams = [{"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": 1510603538107, "measures": {"location": {"val": [-122.69081962885704, 45.52110054870811], "dtype": "float"}, "measure2": {"val": 13, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "chill"}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}, {"stream_name": "driver_data", "version": 0, "stream_token": "abc123","sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {},"timestamp": 1510603538108, "measures": {"location": {"val": [-132.69081962885704, 55.52110054870811], "dtype": "float"}, "measure2": {"val": 9, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "big mood"}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}, {"stream_name": "driver_data", "version": 0, "stream_token": "abc123","sources": {}, 'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {},"timestamp": 1510603538109, "measures": {"location": {"val": [-142.69081962885704, 65.52110054870811], "dtype": "float"}, "measure2": {"val": 4, "dtype": "int"}}, "fields": {"region-code": "PDX"}, "user_ids": {"driver-id": "Molly Mora", "id": 0}, "tags": {"mood": "the last big mood"}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}}]
        self.bstream = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {},'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": [1510603538107, 1510603538108, 1510603538109], "measures":{"location": {"val": [[-122.69081962885704, 45.52110054870811], [-132.69081962885704, 55.52110054870811], [-142.69081962885704, 65.52110054870811]], "dtype": "float"}, "measure2": {"val": [13, 9, 4], "dtype": "int"}}, "fields": {"region-code": ["PDX", "PDX", "PDX"]}, "user_ids": {"driver-id": ["Molly Mora", "Molly Mora", "Molly Mora"], "id": [0, 0, 0]}, "tags": {"mood": ["chill", "big mood", "the last big mood"]}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "template_id": "chadwick666"}
        self.derived_bstream = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {},'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": [1510603538107, 1510603538108, 1510603538109], "derived_measures":{"location": {"val": [[-122.69081962885704, 45.52110054870811], [-132.69081962885704, 55.52110054870811], [-142.69081962885704, 65.52110054870811]], "dtype": "float"}, "measure2": {"val": [13, 9, 4], "dtype": "int"}}, "fields": {"region-code": ["PDX", "PDX", "PDX"]}, "user_ids": {"driver-id": ["Molly Mora", "Molly Mora", "Molly Mora"], "id": [0, 0, 0]}, "tags": {"mood": ["chill", "big mood", "the last big mood"]}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "template_id": "chadwick666"}
        self.events_bstream = {"stream_name": "driver_data", "version": 0, "stream_token": "abc123", "sources": {},'storage_rules': {}, 'ingest_rules': {}, 'engine_rules': {}, "timestamp": [1510603538107, 1510603538108, 1510603538109], "event_measures":{"location": {"val": [[-122.69081962885704, 45.52110054870811], [-132.69081962885704, 55.52110054870811], [-142.69081962885704, 65.52110054870811]], "dtype": "float"}, "measure2": {"val": [13, 9, 4], "dtype": "int"}}, "fields": {"region-code": ["PDX", "PDX", "PDX"]}, "user_ids": {"driver-id": ["Molly Mora", "Molly Mora", "Molly Mora"], "id": [0, 0, 0]}, "tags": {"mood": ["chill", "big mood", "the last big mood"]}, "foreign_keys": [], "filters": [], "dparam_rules": [], "event_rules": {}, "template_id": "chadwick666"}

    def test_store_json(self):
        inserted_template_id = self.coordinator._storejson(self.dstream_template, 'template')
        inserted_derived_id = self.coordinator._store_json(self.derived_bstream, 'derived')
        inserted_event_id = self.coordinator._store_json(self.events_bstream, 'event')

        queried_template_id = self.coordinator.mongo.get_by_id(inserted_template_id, 'template')
        queried_derived_id = self.coordinator.mongo.get_by_id(inserted_derived_id, 'derived')
        queried_event_id = self.coordinator.mongo.get_by_id(inserted_event_id, 'event')

        self.assertEqual(inserted_template_id, queried_template_id)
        self.assertEqual(inserted_derived_id, queried_derived_id)
        self.assertEqual(inserted_event_id, queried_event_id)

    def test_list_to_bstream(self):
        bstream = self.coordinator._list_to_bstream(self.queried_template, self.dstreams, [1,2,3])

        self.assertEqual(bstream, self.bstream)
        self.assertEqual(bstream.ids, [1,2,3])


