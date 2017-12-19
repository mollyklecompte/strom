import unittest
import numpy as np
from strom.transform.derive_param import *


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
        params["func_params"] = {"window": 5, "angle_change":False}
        params["measure_rules"] = {"target_measure":"viscosity", "output_name":"viscous_difference"}
        self.dc.load_params(params)
        test_data_len = 200
        test_data = np.random.randint(0, 15, (test_data_len,))
        test_measure = {"viscosity": {"val": test_data, "dtype": "int"}}
        self.dc.load_measures(test_measure)

    def test_transform_data(self):
        diff_data = self.dc.transform_data()
        self.assertIn("viscous_difference", diff_data)
        self.dc.params["func_params"]["angle_change"] = True
        diff_data = self.dc.transform_data()
        self.assertIn("viscous_difference", diff_data)

class TestDeriveCumsum(unittest.TestCase):
    def setUp(self):
        self.dc = DeriveCumsum()
        params = {}
        params["func_params"] = {"offset":0}
        params["measure_rules"] = {"target_measure": "viscosity", "output_name": "viscous_cumsum"}
        self.dc.load_params(params)
        test_data_len = 200
        test_data = np.random.randint(0, 15, (test_data_len,))
        test_measure = {"viscosity": {"val": test_data, "dtype": "int"}}
        self.dc.load_measures(test_measure)

    def test_cumsum(self):
        cumsum_array = self.dc.transform_data()
        self.assertIn("viscous_cumsum", cumsum_array)


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
        params["func_params"] = {"window": 1, "distance_func": "euclidean", "swap_lon_lat":False}
        params["measure_rules"] = {"spatial_measure": "grid_loc",
                                  "output_name": "eu_dist"}
        self.dd.load_params(params)
        grid_data = [(0,0), (4,0), (4, 2), (1, 2), (0, 2), (0, 0), (0, 0)]
        location_data = [( 45.52842731628303, -122.69856466134118),
                        ( 45.52820263090598, -122.69855647400034),
                        ( 45.52797794641536, -122.69854816698077),
                        ( 45.52775326192415, -122.69853986002761),
                        ( 45.527528573038424, -122.69853182157617),
                        ( 45.52730388484149, -122.69852372148934),
                        ( 45.52707919664398, -122.69851562146722),
                        ( 45.526854510500065, -122.69850740728799),
                        ( 45.52662982708467, -122.69849904142234),
                        ( 45.52640515113754, -122.69849027934863),
                        ( 45.5261804587446, -122.69848249866877),
                        ( 45.525955783114185, -122.69847379557916),
                        ( 45.52573109650996, -122.69846562875873),
                        ( 45.52550640681456, -122.69845773365758),
                        ( 45.52528173451999, -122.69844884295325),
                        ]
        test_measures = {"grid_loc":{"val": grid_data, "dtype": "tuples"}, "latlon":{"val":location_data, "dtype":"tuple"}}
        self.dd.load_measures(test_measures)

    def test_euclidean(self):
        eu_dict = self.dd.transform_data()
        self.assertIn("eu_dist", eu_dict)
        self.assertEqual(eu_dict["eu_dist"][0], 4.0)

    def test_great_circle(self):
        self.dd.params["func_params"]["distance_func"] = "great_circle"
        self.dd.params["measure_rules"]["spatial_measure"] = "latlon"
        self.dd.params["measure_rules"]["output_name"] = "great_circle_dist"
        gc_dist = self.dd.transform_data()
        self.assertIn("great_circle_dist", gc_dist)
        self.assertIsInstance(gc_dist["great_circle_dist"], np.ndarray)
        self.dd.params["func_params"]["swap_lon_lat"] = True
        gc_dist = self.dd.transform_data()
        self.assertIn("great_circle_dist", gc_dist)
        self.assertIsInstance(gc_dist["great_circle_dist"], np.ndarray)

class TestDeriveHeading(unittest.TestCase):
    def setUp(self):
        self.dh = DeriveHeading()
        params = {}
        params["func_params"] = {"window": 1, "heading_type": "bearing", "units":"deg", "swap_lon_lat":False}
        params["measure_rules"] = {"spatial_measure": "location",
                                   "output_name": "bears"}
        self.dh.load_params(params)
        test_heading = np.array([(0.0, 0.0), (15.0, 0), (15.0, 15.0), (15, 0)])
        test_measures = {"location":{"val":test_heading, "dtype":"tuple"}}
        self.dh.load_measures(test_measures)

    def test_bearing(self):
        bears = self.dh.transform_data()
        self.assertIn("bears", bears)
        self.dh.params["func_params"]["units"] = "rad"
        bears = self.dh.transform_data()
        self.assertIn("bears", bears)

    def test_flat_angle(self):
        self.dh.params["func_params"]["heading_type"] = "flat_angle"
        self.dh.params["measure_rules"]["output_name"] = "angle"
        fa = self.dh.transform_data()
        self.assertIn("angle", fa)
        self.dh.params["func_params"]["units"] = "rad"
        fa = self.dh.transform_data()
        self.assertIn("angle", fa)

class TestDeriveWindowSum(unittest.TestCase):
    def setUp(self):
        self.dws = DeriveWindowSum()
        params = {}
        params["func_params"] = {"window": 3}
        params["measure_rules"] = {"target_measure": "viscosity", "output_name": "viscous_winsum"}
        self.dws.load_params(params)
        measures = {"viscosity":{"val":np.ones(10),"dtype":"decimil"}}
        self.dws.load_measures(measures)
        self.measures = measures
    def test_window_sum(self):
        win_sum = self.dws.transform_data()
        self.assertIn("viscous_winsum", win_sum)
        print(win_sum["viscous_winsum"])
        self.dws.params["func_params"]["window"] = 5
        print(self.dws.transform_data()["viscous_winsum"])
        self.dws.params["func_params"]["window"] = 4
        print(self.dws.transform_data()["viscous_winsum"])

class TestDeriveScaled(unittest.TestCase):
    def setUp(self):
        self.ds = DeriveScaled()
        params = {}
        params["func_params"] = {"scalar":0}
        params["measure_rules"] = {"target_measure": "viscosity", "output_name": "viscous_scale"}
        self.ds.load_params(params)
        measures = {"viscosity":{"val":np.ones(10),"dtype":"decimil"}}
        self.ds.load_measures(measures)
        self.measures = measures

    def test_scale_data(self):
        scaled_data = self.ds.transform_data()
        self.assertIn("viscous_scale", scaled_data)
        self.assertTrue(np.all(scaled_data["viscous_scale"] == np.zeros(10)))

class TestInBox(unittest.TestCase):
    def setUp(self):
        self.dib = DeriveInBox()
        params = {}
        params["func_params"] = {"upper_left_corner": (2,3), "lower_right_corner":(3,2)}
        params["measure_rules"] = {"spatial_measure":"line_data", "output_name":"box_check"}
        self.dib.load_params(params)
        measures = {"line_data":{"val":list(zip(range(5),range(5))), "dtype":"decimil"}}
        self.dib.load_measures(measures)
        self.measures = measures

    def test_in_box(self):
        boxy_data = self.dib.transform_data()
        self.assertIn("box_check", boxy_data)
        self.assertEqual(boxy_data["box_check"][0], 0)
        self.assertEqual(boxy_data["box_check"][3], 1)


if __name__ == "__main__":
    unittest.main()
