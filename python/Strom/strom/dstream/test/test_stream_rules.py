import unittest

from strom.dstream.stream_rules import *


class TestRuleDict(unittest.TestCase):
    def setUp(self):
        self.expected_keys = ["name", "noise"]
        self.rd = RuleDict({}, expected_keys=self.expected_keys)

    def test_init(self):
        self.assertIsInstance(self.rd, RuleDict)
        for key in self.expected_keys:
            self.assertIn(key, self.rd)

    def test_keys(self):
        rd2 = RuleDict({"name":"David", "feet":2}, expected_keys = self.expected_keys)
        self.assertIn("name", rd2)
        self.assertEqual(rd2["name"], "David")
        self.assertNotIn("feet", rd2)
        self.assertIsNone(rd2["noise"])

class TestFilterRules(unittest.TestCase):
    def setUp(self):
        self.filter = FilterRules()

    def test_init(self):
        self.assertEqual(self.filter.get_expected_keys(), ['transform_id', 'partition_list', 'measure_list', 'transform_type', 'transform_name', 'param_dict', 'logical_comparison', ])

class TestDParamRules(unittest.TestCase):
    def setUp(self):
        self.dpr = DParamRules()

    def test_init(self):
        self.assertEqual(self.dpr.get_expected_keys(), ['transform_id', 'partition_list', 'measure_list', 'transform_type', 'transform_name', 'param_dict', 'logical_comparison', ])

class TestEventRules(unittest.TestCase):
    def setUp(self):
        self.er = EventRules()

    def test_init(self):
        self.assertEqual(self.er.get_expected_keys(), ['event_id', 'partition_list', 'measure_list', 'transform_type', 'transform_name', 'param_dict', 'logical_comparison', ])

class TestCallbackRules(unittest.TestCase):
    def setUp(self):
        self.cr = CallbackRules()

    def test_init(self):
        self.assertEqual(self.cr.get_expected_keys(),["callback_name", "callback_measures", "callback_filter_measures", "callback_derived_measures"])

class TestStorageRules(unittest.TestCase):
    def setUp(self):
        self.sr = StorageRules()

    def test_init(self):
        self.assertEqual(self.sr.get_expected_keys(), ["store_raw", "store_filtered", "store_derived"])

class TestIngestionRules(unittest.TestCase):
    def setUp(self):
        self.ir = IngestionRules()

    def test_init(self):
        self.assertEqual(self.ir.get_expected_keys(), ["nan_handling", "missing_handling",])

class TestEngineRules(unittest.TestCase):
    def setUp(self):
        self.er = EngineRules()

    def test_init(self):
        self.assertEqual(self.er.get_expected_keys(), ["kafka"])

if __name__ == "__main__":
    unittest.main()
