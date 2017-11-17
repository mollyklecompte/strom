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
                        ( 45.525057049377914, -122.69844057234486),
                        ( 45.52483236282614, -122.69843240969594),
                        ( 45.52460767798184, -122.69842414978574),
                        ( 45.5243829945024, -122.6984157877595),
                        ( 45.52415831102235, -122.69840742580006),
                        ( 45.52393364005846, -122.6984005858238),
                        ( 45.52371074790476, -122.69836793313456),
                        ( 45.52349176963333, -122.6982961525053),
                        ( 45.52342606098393, -122.6980191096492),
                        ( 45.523386541484214, -122.69770333181667),
                        ( 45.52336983497432, -122.69738353438083),
                        ( 45.523354091124546, -122.69706352309416),
                        ( 45.52333850816639, -122.69674349588666),
                        ( 45.52332352619525, -122.69642340792679),
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



if __name__ == "__main__":
    unittest.main()
