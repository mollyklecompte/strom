import unittest
from strom.transform_rules_builder import *


__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class TestTransformRulesBuilder(unittest.TestCase):
    def setUp(self):
        self.test_filter_builder = FilterBuilder()
        self.test_dparam_builder = DParamBuilder()
        self.test_param_inputs_windowsum_1 = {"partition_list": [], "measure_list": ["timestamp"], "target_measure": "timestamp", "output_name": "time_window_sum"}
        self.test_param_inputs_windowsum_2 = {"partition_list": [],"window_len": 4, "measure_list": ["timestamp"], "target_measure": "timestamp", "output_name": "time_window_sum", 'logical_comparison': 'OR',}
        self.test_param_inputs_windowsum_3 = {"partition_list": [],"shmindow_len": 4, "measure_list": ["timestamp"], "target_measure": "timestamp", "output_name": "time_window_sum"}
        self.test_param_inputs_windowsum_bad = {"partition_list": [], "target_measure":"timestamp","output_name":"time_window_sum"}
        self.test_param_inputs_windowsum2_outers = {
            "measure_list": ["timestamp"], "partition_list": [], 'logical_comparison': 'OR',}
        self.test_filter_inputs_butter_1 = {"partition_list": [], "measure_list": ["timestamp"]}
        self.test_filter_inputs_butter_2 = {"partition_list": [], "measure_list": ["timestamp"], "nyquist": 2, "order": 3}
        self.test_filter_inputs_butter_3 = {"partition_list": [], "measure_list": ["timestamp"], "nyquist": 2, "shmorder": 3}
        self.test_filter_inputs_butter_bad = { "measure_list": ["timestamp"], "nyquist": 2, "order": 3}

    def test_create_id(self):
        id = self.test_filter_builder._create_id()
        self.assertIsInstance(id, str)
        self.assertEqual(len(id), 36)
        id2 = self.test_dparam_builder._create_id()
        self.assertIsInstance(id, str)
        self.assertEqual(len(id2), 36)

    def test_get_transform_type(self):
        t = self.test_filter_builder._get_transform_type()
        self.assertEqual(t,"filter_data")
        t2 = self.test_dparam_builder._get_transform_type()
        self.assertEqual(t2,"derive_param")

    def test_get_transform_name(self):
        n = self.test_dparam_builder._get_transform_name("slope")
        self.assertEqual(n, "DeriveSlope")
        n2 = self.test_filter_builder._get_transform_name("butter_lowpass")
        self.assertEqual(n2, "ButterLowpass")

    def test_sort_keys(self):
        result = self.test_filter_builder._sort_keys(self.test_filter_inputs_butter_2)
        self.assertDictEqual(result[0], {"partition_list": [], "measure_list": ["timestamp"], 'logical_comparison': 'AND'})
        self.assertDictEqual(result[1], {"nyquist": 2, "order": 3})
        result2 = self.test_dparam_builder._sort_keys(self.test_param_inputs_windowsum_2)
        self.assertDictEqual(result2[0], {"partition_list": [], "measure_list": ["timestamp"], 'logical_comparison': 'OR'})
        self.assertDictEqual(result2[1], {"window_len": 4, "target_measure": "timestamp", 'output_name': 'time_window_sum'})

    def test_validate_base_inputs(self):
        with self.assertRaises(ValueError):
            self.test_filter_builder._validate_base_inputs(self.test_filter_inputs_butter_bad)
        with self.assertRaises(ValueError):
            self.test_dparam_builder._validate_base_inputs(self.test_param_inputs_windowsum_bad)

    def test_validate_params(self):
        r = self.test_filter_builder._validate_params(self.test_filter_inputs_butter_3, "butter_lowpass")
        self.assertDictEqual(r, {"nyquist": 2})
        r2 = self.test_dparam_builder._validate_params(self.test_param_inputs_windowsum_3, "windowsum")
        self.assertDictEqual(r2, {"target_measure": "timestamp", 'output_name': 'time_window_sum'})

    def test_build_param_dict(self):
        butter_ps = self.test_filter_builder._sort_keys(self.test_filter_inputs_butter_2)[1]
        pd_1 = self.test_filter_builder._build_param_dict("butter_lowpass", butter_ps)
        self.assertDictEqual(pd_1, {'order': 3, 'nyquist': 2, 'filter_name': '_buttered'})
        window_ps = self.test_dparam_builder._sort_keys(self.test_param_inputs_windowsum_2)[1]
        pd_2 = self.test_dparam_builder._build_param_dict("windowsum", window_ps)
        self.assertDictEqual(pd_2, {'func_params': {'window_len': 4}, 'measure_rules': {'target_measure': 'timestamp', 'output_name': 'time_window_sum'}}
)

    def test_builder_rules_dict(self):
        r = self.test_filter_builder.build_rules_dict('butter_lowpass', self.test_filter_inputs_butter_1)
        r['transform_id'] = "blep"
        self.assertDictEqual(r,  {'partition_list': [], 'measure_list': ['timestamp'], 'logical_comparison': 'AND', 'param_dict': {'order': 2, 'nyquist': 0.05, 'filter_name': '_buttered'}, 'transform_id': 'blep', 'transform_type': 'filter_data', 'transform_name': None})

        r2 = self.test_dparam_builder.build_rules_dict('windowsum', self.test_param_inputs_windowsum_1)
        r2['transform_id'] = "bloop"
        self.assertDictEqual(r2, {'partition_list': [], 'measure_list': ['timestamp'], 'logical_comparison': 'AND', 'param_dict': {'func_params': {'window_len': 2}, 'measure_rules': {'target_measure': 'timestamp', 'output_name': 'time_window_sum'}}, 'transform_id': 'bloop', 'transform_type': 'derive_param', 'transform_name': None})

        r3 = self.test_filter_builder.build_rules_dict('butter_lowpass', self.test_filter_inputs_butter_2)
        r3['transform_id'] = "meh"
        self.assertDictEqual(r3, {'partition_list': [], 'measure_list': ['timestamp'], 'logical_comparison': 'AND', 'param_dict': {'order': 3, 'nyquist': 2, 'filter_name': '_buttered'}, 'transform_id': 'meh', 'transform_type': 'filter_data', 'transform_name': None})

        r4 = self.test_dparam_builder.build_rules_dict('windowsum', self.test_param_inputs_windowsum_2)
        r4['transform_id'] = "guh"
        self.assertDictEqual(r4, {'partition_list': [], 'measure_list': ['timestamp'], 'logical_comparison': 'OR', 'param_dict': {'func_params': {'window_len': 4}, 'measure_rules': {'target_measure': 'timestamp', 'output_name': 'time_window_sum'}}, 'transform_id': 'guh', 'transform_type': 'derive_param', 'transform_name': None})

