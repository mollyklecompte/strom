import unittest
from Strom.strom.dstream.filter import Filter


class TestFilter(unittest.TestCase):
    def setUp(self):
        self.filter = Filter()
        print(self.filter)

    def test_init(self):
        self.assertIsInstance(self.filter, Filter)
        init_keys = ["dtype", "filter_name", "func_params"]
        for item in init_keys:
            self.assertIn(item, self.filter.keys())

    def test_set_dtype(self):
        dtype = "float(10, 2)"
        self.filter._set_dtype(dtype)
        self.assertEqual(self.filter["dtype"], dtype)

    def test_set_filter_name(self):
        filter_name = "filter_name_1"
        self.filter._set_filter_name(filter_name)
        self.assertEqual(self.filter["filter_name"], filter_name)

    def test_set_func_params(self):
        func_param = "func_param_1"
        self.filter._set_func_params(func_param)
        self.assertEqual(self.filter["func_params"], func_param)

if __name__ == "__main__":
    unittest.main()
