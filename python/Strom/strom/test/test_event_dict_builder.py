import unittest

from strom.event_dict_builder import Event
from strom.funner_factory import create_turn_rules

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class TestEvent(unittest.TestCase):
    def test_init(self):
        turn = Event(create_turn_rules,
                     base_measure_types=['geo'],
                     required_input_settings=['turn_value'],
                     default_settings={
                         'units': "deg",
                         'heading_type': "bearing",
                         'swap_lon_lat': True,
                         'window_len': 1, 'logical_comparision': "AND"})

        self.assertEqual(turn['callback'], create_turn_rules)
        self.assertEqual(turn['required_event_inputs'], ['partition_list', 'stream_id'])
        for i in ['base_measure_types', 'required_input_settings', 'default_settings']:
            self.assertIn(i, turn.keys())

    def test_validate_keys(self):
        with self.assertRaises(ValueError):
            turn = Event(create_turn_rules, base_measure_types=['geo'], required_input_settings=['turn_value'])

        with self.assertRaises(ValueError):
            turn = Event(create_turn_rules, base_measure_types=['geo'], default_settings={'not': 'me'})

        with self.assertRaises(ValueError):
            turn = Event(create_turn_rules, required_input_settings=['turn_value'], default_settings={'not': 'me'})

    def test_export_fields(self):
        turn = Event(create_turn_rules, base_measure_types=['geo'],
                     required_input_settings=['turn_value'],
                     default_settings={'units': "deg", 'heading_type': "bearing",
                                       'swap_lon_lat': True, 'window_len': 1,
                                       'logical_comparision': "AND"})

        fields = ['base_measure_types', 'required_input_settings', 'default_settings']

        efffs = {'base_measure_types': ['geo'],
                 'required_input_settings': ['turn_value'],
                 'default_settings': {
                                    'units': 'deg',
                                    'heading_type': 'bearing',
                                    'swap_lon_lat': True,
                                    'window_len': 1,
                                    'logical_comparision': 'AND'
                                    }
                 }


        effs = turn.export_fields(*fields)
        self.assertDictEqual(effs, efffs)