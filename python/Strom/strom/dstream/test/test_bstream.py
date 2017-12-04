import unittest
import json
from copy import deepcopy
from strom.dstream.bstream import DStream, BStream

class TestBStream(unittest.TestCase):
    def setUp(self):
        #import demo data
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template.txt"))
        self.dstream_template["_id"] = "chadwick666"
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)

    def test_init(self):
        self.assertEqual(self.bstream["template_id"], "chadwick666")
        self.dstream_template.pop("_id")
        self.assertRaises(KeyError,lambda: BStream(self.dstream_template, self.dstream_template))

    def test_load_from_dict(self):
        self.assertEqual(self.bstream["stream_name"], self.dstream_template["stream_name"])


    def test_aggregate_measures(self):
        self.bstream._aggregate_measures()
        for measure in self.dstream_template["measures"].keys():
            self.assertIn(measure, self.bstream["measures"])
            self.assertIsInstance(self.bstream["measures"][measure]["val"], list)
            self.assertEqual(len(self.bstream["measures"][measure]["val"]),len(self.dstreams))


    def test_aggregate_uids(self):
        self.bstream._aggregate_uids()
        for uuid in self.dstream_template["user_ids"]:
            self.assertIn(uuid, self.bstream["user_ids"])
            self.assertIsInstance(self.bstream["user_ids"][uuid], list)
            self.assertEqual(len(self.bstream["user_ids"][uuid]), len(self.dstreams))

    def test_aggregate_ts(self):
        self.bstream._aggregate_ts()
        self.assertIsInstance(self.bstream["timestamp"], list)
        self.assertEqual(len(self.bstream["timestamp"]), len(self.dstreams))

    def test_aggregate_fields(self):
        self.bstream._aggregate_fields()

        for cur_field in self.dstream_template["fields"]:
            self.assertIn(cur_field, self.bstream["fields"])
            self.assertIsInstance(self.bstream["fields"][cur_field], list)
            self.assertEqual(len(self.bstream["fields"][cur_field]), len(self.dstreams))

    def test_aggregate_tags(self):
        self.bstream._aggregate_tags()

        for tag in self.dstream_template["tags"]:
            self.assertIn(tag, self.bstream["tags"])
            self.assertIsInstance(self.bstream["tags"][tag], list)
            self.assertEqual(len(self.bstream["tags"][tag]), len(self.dstreams))
    #
    def test_aggregate(self):
        b = self.bstream.aggregate
        self.assertIsInstance(self.bstream["timestamp"], list)
        for uuid in self.dstream_template["user_ids"]:
            self.assertIn(uuid, self.bstream["user_ids"])
            self.assertIsInstance(self.bstream["user_ids"][uuid], list)
            self.assertEqual(len(self.bstream["user_ids"][uuid]), len(self.dstreams))
        for measure in self.dstream_template["measures"].keys():
            self.assertIn(measure, self.bstream["measures"])
            self.assertIsInstance(self.bstream["measures"][measure]["val"], list)
            self.assertEqual(len(self.bstream["measures"][measure]["val"]),len(self.dstreams))
        for cur_field in self.dstream_template["fields"]:
            self.assertIn(cur_field, self.bstream["fields"])
            self.assertIsInstance(self.bstream["fields"][cur_field], list)
            self.assertEqual(len(self.bstream["fields"][cur_field]), len(self.dstreams))
        for tag in self.dstream_template["tags"]:
            self.assertIn(tag, self.bstream["tags"])
            self.assertIsInstance(self.bstream["tags"][tag], list)
            self.assertEqual(len(self.bstream["tags"][tag]), len(self.dstreams))
        self.assertEqual(b, self.bstream)

    def test_applying(self):
        bstream = deepcopy(self.bstream)
        bstream.aggregate

        bstream.apply_filters()
        self.assertIsInstance(bstream["filter_measures"], dict)
        for cur_filter in self.dstream_template["filters"]:
            self.assertIn(cur_filter["filter_name"], bstream["filter_measures"])
            self.assertIsInstance(bstream["filter_measures"][cur_filter["filter_name"]]["val"], list)

        bstream.apply_dparam_rules()
        self.assertIsInstance(bstream["derived_measures"], dict)
        for dparam_rule in self.dstream_template["dparam_rules"]:
            self.assertIn(dparam_rule["measure_rules"]["output_name"], bstream["derived_measures"])
            self.assertIsInstance(bstream["derived_measures"][dparam_rule["measure_rules"]["output_name"]]["val"], list)

        bstream.find_events()
        self.assertIsInstance(bstream["events"], dict)
        for cur_event in self.dstream_template["event_rules"].values():
            self.assertIn(cur_event["event_name"], bstream["events"])
            self.assertIsInstance(bstream["events"][cur_event["event_name"]], list)
            self.assertIsInstance(bstream["events"][cur_event["event_name"]][0], dict)


if __name__ == "__main__":
    unittest.main()
