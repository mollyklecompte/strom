import json
import unittest
from copy import deepcopy

import pandas as pd

from strom.dstream.bstream import BStream


class TestBStream(unittest.TestCase):
    def setUp(self):
        #import demo data
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template_unit_test.txt"))
        self.dstream_template["_id"] = "chadwick666"
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)

    def test_init(self):
        self.assertEqual(self.bstream["template_id"], "chadwick666")
        self.dstream_template.pop("_id")
        self.assertRaises(KeyError,lambda: BStream(self.dstream_template, self.dstream_template))

    def test_load_from_dict(self):
        self.assertEqual(self.bstream["stream_name"], self.dstream_template["stream_name"])

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
        self.assertEqual(len(self.bstream["fields"]), len(self.dstreams))

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
        for cur_field in self.dstream_template["fields"]:
            self.assertEqual(len(self.bstream["fields"]), len(self.dstreams))
        for tag in self.dstream_template["tags"]:
            self.assertIn(tag, self.bstream["tags"])
            self.assertIsInstance(self.bstream["tags"][tag], list)
            self.assertEqual(len(self.bstream["tags"][tag]), len(self.dstreams))
        for measure in self.dstream_template["measures"].keys():
            self.assertIn(measure, self.bstream["measures"].columns)
            self.assertIsInstance(self.bstream["measures"], pd.DataFrame)
            self.assertEqual(self.bstream["measures"].shape[0],len(self.dstreams))
        self.assertEqual(b, self.bstream)


    def test_prune_dstreams(self):
        self.assertIsInstance(self.bstream.dstreams, list)
        self.bstream.prune_dstreams()
        self.assertIsNone(self.bstream.dstreams)

    def test_parition_rows(self):
        bstream = deepcopy(self.bstream)
        bstream.aggregate



    def test_apply_filters(self):
        bstream = deepcopy(self.bstream)
        bstream.aggregate

        bstream.apply_filters()
        for cur_filter in self.dstream_template["filters"]:
            for col_name in cur_filter["measure_list"]:
                self.assertIn(col_name+cur_filter["param_dict"]["filter_name"], bstream["measures"].columns)

        bstream.apply_dparam_rules()
        for dparam_rule in self.dstream_template["dparam_rules"]:
            self.assertIn(dparam_rule["param_dict"]["measure_rules"]["output_name"], bstream["measures"].columns)

        # bstream.find_events()
        # self.assertIsInstance(bstream["events"], dict)
        # for cur_event in self.dstream_template["event_rules"].values():
        #     self.assertIn(cur_event["event_name"], bstream["events"])
        #     self.assertIsInstance(bstream["events"][cur_event["event_name"]], list)
        #     self.assertIsInstance(bstream["events"][cur_event["event_name"]][0], dict)


if __name__ == "__main__":
    unittest.main()
