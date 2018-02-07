import json
import unittest

from strom.dstream.bstream import BStream
from strom.transform.filter_dataframe import *


class TestFilter(unittest.TestCase):
    def setUp(self):
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template_unit_test.txt"))
        self.dstream_template["_id"] = "crowley666"
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)
        self.bstream.aggregate


    def test_butter(self):
        butter_rules = {"partition_list":[],
         "measure_list":["timestamp"],
         "transform_type":"filter_data",
         "transform_name": "ButterLowpass",
         "param_dict":{
             "order":2,
             "nyquist": 0.01,
             "filter_name": "_buttery"
         },
         "logical_comparision": "AND"
         }

        butter_df = ButterLowpass(self.bstream["measures"][butter_rules["measure_list"]], butter_rules["param_dict"])
        for measure_name in butter_rules["measure_list"]:
            self.assertIn(measure_name+butter_rules["param_dict"]["filter_name"], butter_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], butter_df.shape[0])


    def test_window(self):
        window_rule = {"partition_list":[],
         "measure_list":["timestamp"],
         "transform_type":"filter_data",
         "transform_name": "WindowAverage",
         "param_dict":{"window_len":3, "filter_name":"_winning"},
         "logical_comparision": "AND"}

        window_df = WindowAverage(self.bstream["measures"][window_rule["measure_list"]], window_rule["param_dict"])
        for measure_name in window_rule["measure_list"]:
            self.assertIn(measure_name+window_rule["param_dict"]["filter_name"], window_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], window_df.shape[0])



if __name__ == "__main__":
    unittest.main()
