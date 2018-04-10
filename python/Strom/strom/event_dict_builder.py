from strom.funner_factory import *


__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"


class Event(dict):
    def __init__(self, builder_callback, **kwargs):
        super().__init__()
        self['callback'] = builder_callback
        self['required_event_inputs'] = ['partition_list', 'stream_id']
        for k, v in kwargs.items():
            self[k] = v
        self._validate_keys()

    def _validate_keys(self):
        missing = []
        for i in ['base_measure_types', 'required_input_settings', 'default_settings']:
            if i not in self.keys():
                missing.append(i)
            if len(missing) > 0:
                raise ValueError(f"Missing keys {[i for i in missing]}")

    def export_fields(self, *keys):
        return {k: self[k] for k in keys}


event_builder_rules = {
    'turn': Event(create_turn_rules,
                  base_measure_types=['geo'],
                  required_input_settings=['turn_value'],
                  default_settings={
                      'units': "deg",
                      'heading_type': "bearing",
                      'swap_lon_lat': True,
                      'window_len': 1,
                      'logical_comparision': "AND"
                  })
}
