import unittest
from Strom.strom.dstream.filter import Filter


class TestFilter(unittest.TestCase):
    def setUp(self):
        self.filter = Filter()
        print(self.filter)

    def test_init(self):
        self.assertIsInstance(self.filter, Filter)
        init_keys = ["dtype", "func_param_name"]
        for item in init_keys:
            self.assertIn(item, self.filter.keys())

    def test_set_dtype(self):
        dtype = "float(10, 2)"
        self.filter._set_dtype(dtype)
        self.assertEqual(self.filter["dtype"], dtype)

    def test_set_func_param_name(self):
        func_param_name = "func_param_name_1"
        self.filter._set_func_param_name(func_param_name)
        self.assertEqual(self.filter["func_param_name"], func_param_name)

if __name__ == "__main__":
    unittest.main()
