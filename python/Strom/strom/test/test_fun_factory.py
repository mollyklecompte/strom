import unittest
import json
from strom.fun_factory import *
from strom.dstream.dstream import DStream

class TestFunFactory(unittest.TestCase):
    def setUp(self):
        demo_data_dir = "demo_data/"
        self.dstream_dict = json.load(open(demo_data_dir + "demo_template_unit_test.txt"))
        self.dstream = DStream()
        self.dstream.load_from_json(self.dstream_dict)
        self.dstream['filters'][0]['transform_id'] = 1
        self.dstream['filters'][1]['transform_id'] = 2
        counter = 1
        for dparam in self.dstream['dparam_rules']:
            dparam['transform_id'] = counter
            counter += 1
        self.test_event_rules = {

        "partition_list": [],
        "measure_list":["timestamp", "head1"],
        "transform_type":"detect_event",
        "transform_name":"DetectThreshold",
        "param_dict":{
            "event_rules":{
                "measure":"head1",
                "threshold_value":69.2,
                "comparison_operator":">=",
                "absolute_compare":True
            },
            "event_name":"nice_event",
            "stream_id":"abc123",
        },
        "logical_comparison": "AND"
        }

        self.test_dparam_rules_list = [
        {
            "partition_list": [("timestamp", 1510603551106, ">"), ("timestamp", 1510603551391, "<")],
            "measure_list":["timestamp", "timestamp_winning"],
            "transform_type": "derive_param",
            "transform_name": "DeriveSlope",
            "param_dict":{
                "func_params":{"window_len":1},
                "measure_rules":{"rise_measure":"timestamp_winning", "run_measure":"timestamp","output_name":"time_slope"}
                },
            "logical_comparison":"AND"
        },
        {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveChange",
            "param_dict":{
                "func_params":{"window_len":1, "angle_change":False},
                "measure_rules":{"target_measure":"timestamp","output_name":"time_change"}
                },
            "logical_comparison":"AND"
        },
        {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveCumsum",
            "param_dict":{
                "func_params":{"offset":0},
                "measure_rules":{"target_measure":"timestamp","output_name":"time_sum"}
                },
            "logical_comparison":"AND"
        },
        {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveWindowSum",
            "param_dict":{
                "func_params":{"window_len":3},
                "measure_rules":{"target_measure":"timestamp","output_name":"time_window_sum"}
                },
            "logical_comparison":"AND"
        },
        {
            "partition_list":[],
            "measure_list":["timestamp",],
            "transform_type": "derive_param",
            "transform_name": "DeriveScaled",
            "param_dict":{
                "func_params":{"scalar":-1},
                "measure_rules":{"target_measure":"timestamp","output_name":"negatime"}
                },
            "logical_comparison":"AND"
        },
        {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveDistance",
            "param_dict":{
                "func_params":{"window_len":1, "distance_func":"euclidean", "swap_lon_lat":True},
                "measure_rules":{"spatial_measure":"location","output_name":"dist1"}
                },
            "logical_comparison":"AND"
        },
         {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveDistance",
            "param_dict":{
                "func_params":{"window_len":1, "distance_func":"great_circle", "swap_lon_lat":True},
                "measure_rules":{"spatial_measure":"location","output_name":"dist2"}
                },
            "logical_comparison":"AND"
        },
        {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveHeading",
            "param_dict":{
                "func_params":{"window_len":1, "units":"deg","heading_type":"bearing", "swap_lon_lat":True},
                "measure_rules":{"spatial_measure":"location","output_name":"head1"}
                },
            "logical_comparison":"AND"
        },
            {
            "partition_list":[],
            "measure_list":["location",],
            "transform_type": "derive_param",
            "transform_name": "DeriveHeading",
            "param_dict":{
                "func_params":{"window_len":1, "units":"deg","heading_type":"flat_angle", "swap_lon_lat":True},
                "measure_rules":{"spatial_measure":"location","output_name":"head2"}
                },
            "logical_comparison":"AND"
        },
        {
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
    ]

    def test_create_template(self):
        t1 = create_template('tester', 'driver_id',[('location', 'geo')], ['driver-id', 'idd'], [('test_event', self.test_event_rules)], self.test_dparam_rules_list)

        self.assertEqual(t1['stream_name'], 'tester')
        self.assertEqual(t1['source_key'], 'driver_id')
        self.assertIn('location', t1['measures'].keys())
        self.assertEqual(t1['measures']['location']['dtype'], 'geo')
        self.assertIn('driver-id', t1['user_ids'])
        self.assertIn('idd', t1['user_ids'])
        self.assertDictEqual(t1['storage_rules'], {"store_raw":True, "store_filtered":True, "store_derived":True})
        self.assertIn('test_event', t1['event_rules'].keys())
