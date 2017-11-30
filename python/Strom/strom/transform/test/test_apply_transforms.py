import unittest
import json
from strom.transform.apply_transformer import *
from strom.transform.derive_param import *
from strom.transform.filter_data import *

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
        bstream = json.load(open("demo_data/demo_bstream_trip26.txt"))
        heading_params = {}
        heading_params["func_name"] = "DeriveHeading"
        heading_params["func_type"] = "derive_param"
        heading_params["func_params"] = {"window": 1, "units": "deg", "heading_type": "bearing", "swap_lon_lat":True}
        heading_params["measure_rules"] = {"spatial_measure": "location", "output_name": "bears"}
        heading_params["measures"] = ["location"]
        transformed_data = apply_transformation(heading_params, bstream)
        self.assertIsInstance(transformed_data, dict)
        self.assertIn(heading_params["measure_rules"]["output_name"], transformed_data)

        bstream["derived_measures"] = transformed_data
        event_params = {}
        event_params["func_type"] ="detect_event"
        event_params["func_name"] = "DetectThreshold"
        event_params["event_rules"] = {"measure":"bears", "threshold_value":90, "comparison_operator":">="}
        event_params["event_name"] = "turn90"
        event_params["stream_token"] = bstream["stream_token"]
        event_params["measures"] = ["location"]
        event_params["derived_measures"] = ["bears"]
        event_list = apply_transformation(event_params, bstream)
        self.assertIsInstance(event_list, list)
        for event in event_list:
            self.assertIsInstance(event, dict)
            self.assertEqual(event["event_name"], event_params["event_name"])


if __name__ == "__main__":
    unittest.main()