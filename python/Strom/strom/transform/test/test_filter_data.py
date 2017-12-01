import unittest
import numpy as np
from strom.transform.filter_data import Filter, ButterLowpass, WindowAverage


class TestFilter(unittest.TestCase):
    def setUp(self):
        self.filter = Filter()

    def test_init(self):
        self.assertIsInstance(self.filter.data, dict)
        self.assertIsInstance(self.filter.params, dict)
    def test_params(self):
        param_dict = {"func_params":{"order":"out of order"}, "filter_name":"test_params"}
        self.filter.load_params(param_dict)
        self.assertIsInstance(self.filter.params["func_params"], dict)
        self.assertIn("order", self.filter.params["func_params"])
        self.assertEqual(self.filter.get_params(), param_dict["func_params"])

class TestButter(unittest.TestCase):
    def setUp(self):
        self.butter = ButterLowpass()

    def test_defaults(self):
        self.assertEqual(self.butter.get_params(), {"order":3, "nyquist":0.05})

    def test_butter_data(self):
        measure = {"viscosity":{"val":range(25), "dtype":"int"}}
        self.butter.load_measures(measure)
        buttered_data = self.butter.transform_data()
        self.assertIsInstance(buttered_data, dict)
        self.assertIn("buttered", buttered_data)
        self.assertEqual(len(measure["viscosity"]["val"]), buttered_data["buttered"].shape[0])

class TestWindow(unittest.TestCase):
    def setUp(self):
        self.wa = WindowAverage()

    def test_window(self):
        test_data_len = 200
        test_data = np.random.randint(0,15,(test_data_len,))
        test_measure = {"viscosity":{"val":test_data, "dtype":"int"}}
        self.wa.load_measures(test_measure)
        for test_window in range(1, int(np.floor(test_data_len/2))):
            params = {"func_params":{"window_len":test_window}, "filter_name":"viscosity_windowed"}
            self.wa.load_params(params)
            windowed_data = self.wa.transform_data()
            self.assertIsInstance(windowed_data, dict)
            self.assertIn("viscosity_windowed", windowed_data)
            self.assertEqual(len(test_measure["viscosity"]["val"]), windowed_data["viscosity_windowed"].shape[0], "window len "+str(test_window))


if __name__ == "__main__":
    unittest.main()
