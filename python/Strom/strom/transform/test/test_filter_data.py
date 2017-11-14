import unittest
from Strom.strom.transform.filter_data import Filter, butter_lowpass


class TestFilter(unittest.TestCase):
    def setUp(self):
        self.filter = Filter()

    def test_init(self):
        self.assertIsInstance(self.filter.data, dict)
        self.assertIsInstance(self.filter.params, dict)
    def test_params(self):
        param_dict = {"func_params":{"order":"out of order"}}
        self.filter.load_params(param_dict)
        self.assertIsInstance(self.filter.params["func_params"], dict)
        self.assertIn("order", self.filter.params["func_params"])
        self.assertEqual(self.filter.get_params(), param_dict["func_params"])

class TestButter(unittest.TestCase):
    def setUp(self):
        self.butter = butter_lowpass()

    def test_defaults(self):
        self.assertEqual(self.butter.get_params(), {"order":3, "nyquist":0.05})

    def test_butter_data(self):
        measure = {"viscosity":{"val":range(25), "dtype":"int"}}
        self.butter.load_measures(measure)
        buttered_data = self.butter.transform_data()
        self.assertIsInstance(buttered_data, dict)
        self.assertIn("viscosity", buttered_data)
        self.assertEqual(len(measure["viscosity"]["val"]), buttered_data["viscosity"].shape[0])

if __name__ == "__main__":
    unittest.main()
