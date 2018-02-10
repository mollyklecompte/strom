import unittest
import json
from strom.fun_factory_children import *
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

    def test_update_stream_name(self):
        update_stream_name(self.dstream, 'chadwick')
        self.assertEqual(self.dstream['stream_name'], 'chadwick')

    def test_update_source_key(self):
        update_source_key(self.dstream, 'idd')
        self.assertEqual(self.dstream['source_key'], 'idd')

    def test_update_description(self):
        update_description(self.dstream, 'lmao')
        self.assertEqual(self.dstream['user_description'], 'lmao')

    def test_update_user_id(self):
        update_user_id(self.dstream, 'galactic_id', old_id='driver-id')
        self.assertIn('galactic_id', self.dstream['user_ids'].keys())
        self.assertNotIn('driver-id', self.dstream['user_ids'].keys())

    def test_update_field(self):
        update_field(self.dstream, 'dumb_field')
        self.assertIn('dumb_field', self.dstream['fields'])
        update_field(self.dstream, 'asteroid_field', old_field='dumb_field')
        self.assertNotIn('dumb_field', self.dstream['fields'].keys())
        self.assertIn('asteroid_field', self.dstream['fields'].keys())

    def test_update_tag(self):
        update_tag(self.dstream, 'not_poodles')
        self.assertIn('not_poodles', self.dstream['tags'].keys())
        update_tag(self.dstream, 'poodles', old_tag='not_poodles')
        self.assertIn('poodles', self.dstream['tags'].keys())
        self.assertNotIn('not_poodles', self.dstream['tags'].keys())

    def test_update_fk(self):
        update_foreign_key(self.dstream, 'ugh')
        self.assertIn({'ugh': None}, self.dstream['foreign_keys'])
        update_foreign_key(self.dstream, 'wah', old_fk='ugh')
        self.assertIn({'wah': None}, self.dstream['foreign_keys'])
        self.assertNotIn({'ugh': None}, self.dstream['foreign_keys'])

    def test_update_rules(self):
        update_rules(self.dstream, 'storage_rules', [('store_raw', False), ('store_filtered', False)])
        self.assertDictEqual({"store_raw":False, "store_filtered":False, "store_derived":True}, self.dstream['storage_rules'])
        update_rules(self.dstream, 'ingest_rules', [('is_this_all_fake', True)])
        self.assertIn('is_this_all_fake', self.dstream['ingest_rules'].keys())
        self.assertTrue(self.dstream['ingest_rules']['is_this_all_fake'])

    def test_modify_filter(self):
        modify_filter(self.dstream, 1, [('order', 3), ('nyquist', 0.69)], new_partition_list=['whatever'], change_comparison=True)
        self.assertEqual(self.dstream['filters'][0]['param_dict']['order'], 3)
        self.assertEqual(self.dstream['filters'][0]['param_dict']['nyquist'], 0.69)
        self.assertEqual(self.dstream['filters'][0]['logical_comparison'], 'OR')
        self.assertIn('whatever', self.dstream['filters'][0]['partition_list'])

    def test_modify_dparam(self):
        modify_dparam(self.dstream, 2, [('window_len', 2)])
        self.assertEqual(self.dstream['dparam_rules'][1]['param_dict']['func_params']['window_len'], 2)
        self.assertEqual(len(self.dstream['dparam_rules'][1]['partition_list']), 0)

        modify_dparam(self.dstream, 2, [], new_partition_list=['new'])
        self.assertEqual(list(self.dstream['dparam_rules'][1]['param_dict']['func_params'].items()), [('window_len', 2), ('angle_change', False)])
        self.assertEqual(self.dstream['dparam_rules'][1]['partition_list'], ['new'])

    def test_modify_event(self):
        modify_event(self.dstream, 'test_event', [('threshold_value', 70), ('absolute_compare', False)])
        self.assertEqual(self.dstream['event_rules']['test_event']['param_dict']['event_rules']['threshold_value'], 70)
        self.assertFalse(self.dstream['event_rules']['test_event']['param_dict']['event_rules']['absolute_compare'])

    def test_remove_transform(self):
        remove_transform(self.dstream, 'filters', 2)
        self.assertEqual(len(self.dstream['filters']), 1)
        self.assertEqual(self.dstream['filters'][0]['transform_name'], 'ButterLowpass')