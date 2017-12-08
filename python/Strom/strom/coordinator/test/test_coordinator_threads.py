import unittest
import json
from copy import deepcopy
from strom.coordinator.coordinator import Coordinator
from strom.dstream.bstream import BStream
from pymysql.err import ProgrammingError
from pymongo.errors import DuplicateKeyError
from strom.dstream.stream_rules import StorageRules
from strom.storage_thread.storage_thread import *

class TestCoordinator(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator()
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template.txt"))
        self.dstream_template["stream_token"] = "abc123"
        self.dstream_template["_id"] = "chadwick666"
        self.dstream = json.load(open(demo_data_dir+"demo_single_data.txt"))
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)
        self.bstream = self.bstream.aggregate
        self.bstream.apply_filters()
        self.bstream.apply_dparam_rules()
        self.bstream.find_events()

    def test_store_raw_filtered_threads(self):
        tsrf_template = deepcopy((self.dstream_template))
        tsrf_bstream = deepcopy(self.bstream)
        tsrf_template.pop("_id", None)
        cur_stream_token = "storing_token_token"
        tsrf_bstream["stream_token"] = cur_stream_token
        tsrf_template["stream_token"] = cur_stream_token

        #Need to figure these out , how to handle with threading??
        #self.assertRaises(ProgrammingError, lambda: self.coordinator._store_raw(tsrf_bstream))

        self.coordinator.process_template(tsrf_template)
        # start thread
        raw_thread = StorageRawThread(tsrf_bstream)
        raw_thread.start()
        raw_thread.join()
        inserted_count = raw_thread.rows_inserted


        #inserted_count = self.coordinator._store_raw(tsrf_bstream)
        self.assertEqual(inserted_count, len(tsrf_bstream["timestamp"]))
        self.assertFalse(inserted_count == 0)
        self.assertIsNotNone(inserted_count)

        qt = self.coordinator._retrieve_current_template(tsrf_template["stream_token"])
        bstream = self.coordinator._list_to_bstream(qt, self.dstreams)
        bstream.apply_filters()

        # test storing filtered data
        #filtered_inserted_count = self.coordinator._store_filtered(bstream)
        filtered_thread = StorageFilteredThread(bstream)
        filtered_thread.start()
        filtered_thread.join()
        filtered_inserted_count = filtered_thread.rows_inserted
        bstream['stream_token'] = "this_token_is_bad"
        #self.assertRaises(ProgrammingError,lambda: self.coordinator._store_filtered(bstream))
        bstream['stream_token'] = cur_stream_token
        self.assertEqual(filtered_inserted_count, len(self.bstream["timestamp"]))

        # checks to make sure raw data with bad stream token raises error during storage attempt
        self.bstream["stream_token"] = "not_a_real_token"
        #self.assertRaises(ProgrammingError, lambda: self.coordinator._store_raw(self.bstream))

    def test_store_json_thread(self):
        #inserted_template_id = self.coordinator._store_json(self.dstream_template, 'template')
        json_thread_template = StorageJsonThread(self.dstream_template, 'template')
        json_thread_template.start()
        json_thread_template.join()
        inserted_template_id = json_thread_template.insert_id

        #inserted_derived_id = self.coordinator._store_json(self.bstream, 'derived')
        json_thread_derived = StorageJsonThread(self.bstream, 'derived')
        json_thread_derived.start()
        json_thread_derived.join()
        inserted_derived_id = json_thread_derived.insert_id

        #inserted_event_id = self.coordinator._store_json(self.bstream, 'event')
        json_thread_event = StorageJsonThread(self.bstream, 'event')
        json_thread_event.start()
        json_thread_event.join()
        inserted_event_id = json_thread_event.insert_id

        queried_template = self.coordinator.mongo.get_by_id(inserted_template_id, 'template')
        queried_derived = self.coordinator.mongo.get_by_id(inserted_derived_id, 'derived', token=self.dstream_template["stream_token"])
        queried_event= self.coordinator.mongo.get_by_id(inserted_event_id, 'event', token=self.dstream_template["stream_token"])

        self.assertEqual(inserted_template_id, queried_template["_id"])
        self.assertEqual(inserted_derived_id, queried_derived["_id"])
        self.assertEqual(inserted_event_id, queried_event["_id"])

    def test_list_to_bstream(self):
        bstream = self.coordinator._list_to_bstream(self.dstream_template, self.dstreams)

        self.assertEqual(bstream["measures"], self.bstream["measures"])


    def test_process_template(self):
        tpt_dstream = deepcopy(self.dstream_template)
        tpt_dstream["stream_token"] = "a_tpt_token"
        tpt_dstream.pop("_id", None)

        self.coordinator.process_template(tpt_dstream)

        qt = self.coordinator._retrieve_current_template(tpt_dstream["stream_token"])
        self.assertEqual(qt["stream_token"], tpt_dstream["stream_token"])
        self.assertEqual(qt["stream_name"], tpt_dstream["stream_name"])

        tpt_dstream["version"] = 1
        ### TODO test with id, raises error ###
        tpt_dstream["_id"] = qt["_id"]
        self.assertRaises(DuplicateKeyError, lambda: self.coordinator.process_template(tpt_dstream))

        tpt_dstream.pop("_id", None)
        tpt_dstream["stream_token"] = "last_tpt_token"
        self.coordinator.process_template(tpt_dstream)
        qt = self.coordinator._retrieve_current_template(tpt_dstream["stream_token"])
        self.assertEqual(qt["version"], 1)

    # def test_process_data_sync(self):
    #     tpds_dstream = deepcopy(self.dstream_template)
    #     # tpds_dstream["stream_token"] = "the final token"
    #     tpds_dstream.pop("_id", None)
    #     self.coordinator.process_template(tpds_dstream)
    #     self.coordinator.process_data_sync(self.dstreams, tpds_dstream["stream_token"])
    #     stored_events = self.coordinator.get_events(tpds_dstream["stream_token"])
    #     self.assertIn("events", stored_events[0])
    #     for event in tpds_dstream["event_rules"].keys():
    #         self.assertIn(event, stored_events[0]["events"])

    def test_process_data_async(self):
        import time
        tpds_dstream = deepcopy(self.dstream_template)
        # tpds_dstream["stream_token"] = "the final token"
        tpds_dstream.pop("_id", None)
        self.coordinator.process_template(tpds_dstream)
        self.coordinator.process_data_async(self.dstreams, tpds_dstream["stream_token"])

        # pause to let threaded storage complete
        time.sleep(1)
        stored_events = self.coordinator.get_events(tpds_dstream["stream_token"])
        self.assertIn("events", stored_events[0])
        for event in tpds_dstream["event_rules"].keys():
            self.assertIn(event, stored_events[0]["events"])

        #verify storage rules (on)
        storage_rules = self.dstream_template['storage_rules']
        self.assertEqual(storage_rules['store_raw'],'raw_thread' in self.coordinator.threads)
        self.assertEqual(storage_rules['store_filtered'],'filtered_thread' in self.coordinator.threads)
        self.assertEqual(storage_rules['store_derived'],'derived_thread' in self.coordinator.threads)
        if storage_rules['store_raw'] :
            self.assertTrue('raw_thread' in self.coordinator.threads)
        if storage_rules['store_filtered'] :
            self.assertTrue('filtered_thread' in self.coordinator.threads)
        if storage_rules['store_derived'] :
            self.assertTrue('derived_thread' in self.coordinator.threads)

    def test_process_data_async_sr_off(self):
        #toggle the storage rules off
        import time

        # new token
        self.dstream_template["stream_token"] = "abc123nosr"
        tpds_dstream = deepcopy(self.dstream_template)

        #toggle the storage rules
        tpds_dstream['storage_rules'] = {'store_raw': False, 'store_filtered': False, 'store_derived': False}
        tpds_dstream.pop("_id", None)
        self.coordinator.process_template(tpds_dstream)
        self.coordinator.process_data_async(self.dstreams, tpds_dstream["stream_token"])

        # pause to let threaded storage complete
        time.sleep(3)
        stored_events = self.coordinator.get_events(tpds_dstream["stream_token"])
        self.assertIn("events", stored_events[0])
        for event in tpds_dstream["event_rules"].keys():
            self.assertIn(event, stored_events[0]["events"])

        # verify storage rules (OFF)
        storage_rules = tpds_dstream['storage_rules']
        print('storage rules off',storage_rules['store_raw'])
        print('storage rules off',storage_rules['store_filtered'])
        print('storage rules off',storage_rules['store_derived'])
        self.assertEqual(storage_rules['store_raw'],'raw_thread' in self.coordinator.threads)
        self.assertEqual(storage_rules['store_filtered'],'filtered_thread' in self.coordinator.threads)
        self.assertEqual(storage_rules['store_derived'],'derived_thread' in self.coordinator.threads)




if __name__ == "__main__":
    unittest.main()