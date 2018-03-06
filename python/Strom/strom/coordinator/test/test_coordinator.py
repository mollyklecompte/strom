import json
import unittest
from copy import deepcopy

from strom.coordinator.coordinator import Coordinator
from strom.dstream.bstream import BStream


class TestCoordinator(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator()
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template_unit_test.txt"))
        self.dstream_template["stream_token"] = "abc123"
        self.dstream_template["_id"] = "chadwick666"
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)
        self.bstream = self.bstream.aggregate
        self.bstream.apply_filters()
        self.bstream.apply_dparam_rules()
        self.bstream.find_events()

    def test_list_to_bstream(self):
        bstream = self.coordinator._list_to_bstream(self.dstream_template, self.dstreams)
        bstream.apply_filters()
        bstream.apply_dparam_rules()
        bstream.find_events()

        self.assertTrue(bstream["measures"].equals(self.bstream["measures"]))

    def test_parse_events(self):
        parsed_events = self.coordinator._parse_events(self.bstream)
        self.assertIsInstance(parsed_events, list)
        for event_dict in parsed_events:
            self.assertIn(event_dict["event"], [event_names.replace(" ","") for event_names in self.bstream["event_rules"].keys()])
            self.assertIsInstance(event_dict["data"], str)

    def test_post_events(self):
        parsed_events = self.coordinator._parse_events(self.bstream)
        status = self.coordinator._post_events(parsed_events[0])
        print(status)
        self.assertEqual(status["request_status"],200)

    def test_post_parsed_events(self):
        self.coordinator._post_parsed_events(self.bstream)

    def test_post_template(self):
        status = self.coordinator._post_template(self.dstream_template)
        self.assertEqual(status["request_status"],200)

    def test_post_dataframe(self):
        status = self.coordinator._post_dataframe(self.bstream["stream_token"], self.bstream["measures"])
        self.assertEqual(status["request_status"],200)

    def test_process_template(self):
        self.coordinator.process_template(self.dstream_template)

    def test_process_data(self):
        pass
        tpds_dstream = deepcopy(self.dstream_template)
        tpds_dstream["stream_token"] = "the final token"
        tpds_dstream.pop("_id", None)
        self.coordinator.process_template(tpds_dstream)



if __name__ == "__main__":
    unittest.main()
