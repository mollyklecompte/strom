import json
import unittest

from strom.dstream.bstream import BStream


class TestDeriveDataFrame(unittest.TestCase):
    def setUp(self):
        demo_data_dir = "demo_data/"
        self.dstream_template = json.load(open(demo_data_dir + "demo_template_unit_test.txt"))
        self.dstream_template["_id"] = "crowley666"
        self.dstreams = json.load(open(demo_data_dir+"demo_trip26.txt"))
        self.bstream = BStream(self.dstream_template, self.dstreams)
        self.bstream.aggregate

    def test_derive_slope(self):
        slope_rules = {
            "partition_list": [],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveSlope",
            "param_dict":{
                "func_params":{"window_len":1},
                "measure_rules":{"rise_measure":"timestamp", "run_measure":"timestamp","output_name":"time_slope"}
                },
            "logical_comparison":"AND"
        }
