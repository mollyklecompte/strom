import json
import unittest

from strom.dstream.bstream import BStream
from strom.transform.detect_dataframe_event import *


class TestDeriveDataFrame(unittest.TestCase):
    def setUp(self):
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template_unit_test.txt"))
        self.dstream_template["_id"] = "crowley666"
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)
        self.bstream.aggregate


    def test_detect_threshold(self):
        threshold_rules = {
        "partition_list": [],
        "measure_list":["timestamp",],
        "transform_type":"detect_event",
        "transform_name":"DetectThreshold",
        "param_dict":{
            "event_rules":{
                "measure":"timestamp",
                "threshold_value":1510603565552,
                "comparison_operator":">=",
                "absolute_compare":True
            },
            "event_name":"nice_event",
            "stream_id":"abc123",
        },
        "logical_comparison": "AND"
        }

        threshold_df = DetectThreshold(self.bstream["measures"][threshold_rules["measure_list"]], threshold_rules["param_dict"])
        self.assertIn("timestamp", threshold_df.columns)
        self.assertIn("stream_id", threshold_df.columns)
        self.assertIn("event_name", threshold_df.columns)


if __name__ == "__main__":
    unittest.main()
