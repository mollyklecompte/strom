import json
import unittest

from strom.dstream.bstream import BStream
from strom.transform.derive_param import *


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
        slope_df = DeriveSlope(self.bstream["measures"][slope_rules["measure_list"]], slope_rules["param_dict"])

        self.assertIn(slope_rules["param_dict"]["measure_rules"]["output_name"], slope_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], slope_df.shape[0])

    def test_derive_change(self):
        change_rules = {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveChange",
            "param_dict":{
                "func_params":{"window_len":1, "angle_change":False},
                "measure_rules":{"target_measure":"timestamp","output_name":"time_change"}
                },
            "logical_comparison":"AND"
        }
        change_df = DeriveChange(self.bstream["measures"][change_rules["measure_list"]], change_rules["param_dict"])
        self.assertIn(change_rules["param_dict"]["measure_rules"]["output_name"], change_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], change_df.shape[0]+1)


    def test_derive_cumsum(self):
        cumsum_rules =  {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveCumsum",
            "param_dict":{
                "func_params":{"offset":0},
                "measure_rules":{"target_measure":"timestamp","output_name":"time_sum"}
                },
            "logical_comparison":"AND"
        }
        cumsum_df = DeriveCumsum(self.bstream["measures"][cumsum_rules["measure_list"]], cumsum_rules["param_dict"])
        self.assertIn(cumsum_rules["param_dict"]["measure_rules"]["output_name"], cumsum_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], cumsum_df.shape[0])


    def test_window_sum(self):
        winsum_rules = {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveWindowSum",
            "param_dict":{
                "func_params":{"window_len":3},
                "measure_rules":{"target_measure":"timestamp","output_name":"time_window_sum"}
                },
            "logical_comparison":"AND"
        }
        winsum_df = DeriveWindowSum(self.bstream["measures"][winsum_rules["measure_list"]], winsum_rules["param_dict"])
        self.assertIn(winsum_rules["param_dict"]["measure_rules"]["output_name"], winsum_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], winsum_df.shape[0])


    def test_derive_scaled(self):
        scaled_rules = {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveScaled",
            "param_dict":{
                "func_params":{"scalar":-1},
                "measure_rules":{"target_measure":"timestamp","output_name":"negatime"}
                },
            "logical_comparison":"AND"
        }
        scaled_df = DeriveScaled(self.bstream["measures"][scaled_rules["measure_list"]], scaled_rules["param_dict"])
        self.assertIn(scaled_rules["param_dict"]["measure_rules"]["output_name"], scaled_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], scaled_df.shape[0])


    def test_derive_dist(self):
        dist_rules = {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveDistance",
            "param_dict":{
                "func_params":{"window_len":1, "distance_func":"euclidean", "swap_lon_lat":True},
                "measure_rules":{"spatial_measure":"location","output_name":"dist1"}
                },
            "logical_comparison":"AND"
        }
        dist_df = DeriveDistance(self.bstream["measures"][dist_rules["measure_list"]], dist_rules["param_dict"])
        self.assertIn(dist_rules["param_dict"]["measure_rules"]["output_name"], dist_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], dist_df.shape[0]+1)

        dist_rules["param_dict"]["func_params"]["distance_func"] = "great_circle"
        dist_df = DeriveDistance(self.bstream["measures"][dist_rules["measure_list"]], dist_rules["param_dict"])
        self.assertIn(dist_rules["param_dict"]["measure_rules"]["output_name"], dist_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], dist_df.shape[0]+1)

    def test_derive_heading(self):
        head_rules = {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveHeading",
            "param_dict":{
                "func_params":{"window_len":1, "units":"deg","heading_type":"bearing", "swap_lon_lat":True},
                "measure_rules":{"spatial_measure":"location","output_name":"head1"}
                },
            "logical_comparison":"AND"
        }

        head_df = DeriveHeading(self.bstream["measures"][head_rules["measure_list"]], head_rules["param_dict"])
        self.assertIn(head_rules["param_dict"]["measure_rules"]["output_name"], head_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], head_df.shape[0]+1)

        head_rules["param_dict"]["func_params"]["heading_type"] = "flat_angle"
        head_df = DeriveHeading(self.bstream["measures"][head_rules["measure_list"]], head_rules["param_dict"])
        self.assertIn(head_rules["param_dict"]["measure_rules"]["output_name"], head_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], head_df.shape[0]+1)


    def test_in_box(self):
        box_rules = {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveInBox",
            "param_dict":{
                "func_params":{"upper_left_corner":(-122.6835826856399, 45.515814287782455), "lower_right_corner":(-122.678529, 45.511597)},
                "measure_rules":{"spatial_measure":"location","output_name":"boxy"}
                },
            "logical_comparison":"AND"
        }

        box_df = DeriveInBox(self.bstream["measures"][box_rules["measure_list"]],
                                box_rules["param_dict"])
        self.assertIn(box_rules["param_dict"]["measure_rules"]["output_name"], box_df.columns)
        self.assertEqual(self.bstream["measures"].shape[0], box_df.shape[0])

if __name__ == "__main__":
    unittest.main()
