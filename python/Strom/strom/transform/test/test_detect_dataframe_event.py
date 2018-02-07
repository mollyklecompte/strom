import unittest
from strom.transform.detect_event import DetectEvent, DetectThreshold

class TestDetectEvent(unittest.TestCase):
    def setUp(self):
        self.de = DetectEvent()
        event_params = {}
        event_params["event_rules"] = {"measure":"change_in_heading", "threshold_value":90, "comparison_operator":">="}
        event_params["event_name"] = "ninety_degree_turn"
        event_params["stream_token"] = "token_stream_token"
        self.event_params = event_params

    def test_params(self):
        self.de.load_params(self.event_params)
        out_param = self.de.get_params()
        self.assertIsInstance(out_param, dict)
        for key, val in self.event_params.items():
            self.assertIn(key, out_param)
            self.assertEqual(val, out_param[key])

    def test_timestamp(self):
        fake_time = [0, 1, 2]
        self.de.add_timestamp(fake_time)
        self.assertIsInstance(self.de.data["timestamp"], dict)
        self.assertEqual(self.de.data["timestamp"]["val"], fake_time)
        self.assertEqual(self.de.data["timestamp"]["dtype"], "decimal")

class TestDetectThreshold(unittest.TestCase):
    def setUp(self):
        self.dt = DetectThreshold()
        event_params = {}
        event_params["event_rules"] = {"measure":"ranger", "threshold_value":5, "comparison_operator":">="}
        event_params["event_name"] = "over5"
        event_params["stream_token"] = "token_stream_token"
        event_m = {}
        event_m["ranger"] =  {"val":range(10), "dtype":"int"}
        event_m["timestamp"] = {"val":range(50,60), "dtype":"int"}
        event_m["context"] = {"val":['a','b','c','d','e','f','g','h','i','j'], "dtype":"varchar"}
        self.dt.load_params(event_params)
        self.dt.load_measures(event_m)

    def test_transform(self):
        event_list = self.dt.transform_data()
        for event in event_list:
            self.assertIn("event_ind", event)
            cur_ind = event["event_ind"]
            self.assertIn("event_context", event)
            self.assertEqual(event["event_context"]["context"], self.dt.data["context"]["val"][cur_ind])



if __name__ == "__main__":
    unittest.main()
