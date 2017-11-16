import unittest
import numpy as np
from Strom.strom.transform.derive_param import DeriveParam, DeriveSlope, DeriveChange, DeriveDistance


class TestDeriveParam(unittest.TestCase):
    def setUp(self):
        self.dp = DeriveParam()

    def test_params(self):
        params = {}
        params["func_params"] = {"window": 1}
        params["measure_rules"] = {"best_measure": "viscosity", "worse_measure": "range(N)"}
        self.dp.load_params(params)
        self.assertIn("func_params", self.dp.params)
        self.assertIn("measure_rules", self.dp.params)
        self.assertEqual(params, self.dp.get_params())

class TestDeriveChange(unittest.TestCase):
    def setUp(self):
        self.dc = DeriveChange()
        params = {}
        params["func_params"] = {"window": 5}
        params["measure_rules"] = {"target_measure":"viscosity", "output_name":"viscous_difference"}
        self.dc.load_params(params)
        test_data_len = 200
        test_data = np.random.randint(0, 15, (test_data_len,))
        test_measure = {"viscosity": {"val": test_data, "dtype": "int"}}
        self.dc.load_measures(test_measure)
    def test_transform_data(self):
        diff_data = self.dc.transform_data()
        self.assertIn("viscous_difference", diff_data)

class TestDeriveSlope(unittest.TestCase):
    def setUp(self):
        self.ds = DeriveSlope()
        params = {}
        params["func_params"] = {"window": 5}
        params["measure_rules"] = {"rise_measure":"viscosity", "run_measure":"ranger", "output_name":"viscous_slope"}
        self.ds.load_params(params)
        test_data_len = 200
        test_data = np.random.randint(0, 15, (test_data_len,))
        test_measure = {"viscosity": {"val": test_data, "dtype": "int"}, "ranger":{"val":range(test_data_len),"dtype":"int"}}
        self.ds.load_measures(test_measure)
    def test_transform_data(self):
        sloped_data = self.ds.transform_data()
        self.assertIn("viscous_slope", sloped_data)

class TestDeriveDistance(unittest.TestCase):
    def setUp(self):
        self.dd = DeriveDistance()
        params = {}
        params["func_params"] = {"window": 1, "distance_func": "euclidean"}
        params["measure_rules"] = {"spatial_measure": "grid_loc",
                                  "output_name": "eu_dist"}
        self.dd.load_params(params)
        grid_data = [(0,0), (4,0), (4, 2), (1, 2), (0, 2), (0, 0), (0, 0)]
        test_measure = {"grid_loc":{"val": grid_data, "dtype": "tuples"}}




if __name__ == "__main__":
    unittest.main()
