import unittest
import json
from Strom.strom.transform.apply_transformer import *
from Strom.strom.transform.derive_param import *
from Strom.strom.transform.filter_data import *

class TestApplyTransformer(unittest.TestCase):
    def setUp(self):
        pass

    def test_map_to_measure(self):
        test_array = np.zeros(6)
        return_array = map_to_measure({"zero": test_array})
        self.assertIsInstance(return_array, dict)
        self.assertIn("zero", return_array)
        self.assertIn("val", return_array["zero"])
        self.assertIn("dtype", return_array["zero"])
        self.assertTrue(np.array_equal(return_array["zero"]["val"], test_array))

    def test_select_filter(self):
        butter = select_filter("ButterLowpass")
        self.assertIsInstance(butter, ButterLowpass)
        window = select_filter("WindowAverage")
        self.assertIsInstance(window, WindowAverage)

    def test_select_dparam(self):
        dparam_list = ["DeriveSlope", "DeriveChange", "DeriveDistance", "DeriveHeading"]
        for dparam in dparam_list:
            tmp_func = select_dparam(dparam)
            self.assertIsInstance(tmp_func, DeriveParam)

    def test_select_transform(self):
        butter = select_transform("filter_data", "ButterLowpass")
        self.assertIsInstance(butter, ButterLowpass)

    def test_apply_transformation(self):
        bstream = json.load(open("Strom/strom/transform/bstream.txt"))
        heading_params = {}
        heading_params["filter_name"] = "bears"
        heading_params["func_name"] = "DeriveHeading"
        heading_params["func_type"] = "derive_param"
        heading_params["func_params"] = {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat":True}
        heading_params["measure_rules"] = {"spatial_measure": "location", "output_name": "bears"}
        heading_params["measures"] = ["location"]
        transformed_data = apply_transformation(heading_params, bstream)
        self.assertIsInstance(transformed_data, dict)
        self.assertIn(heading_params["filter_name"], transformed_data)

