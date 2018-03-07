import uuid
from strom.dstream.stream_rules import DParamRules, FilterRules
from strom.build_param_dicts import build_param_dict


__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"


class TransformRulesBuilder(object):
    def __init__(self):
        self.transform_type = None
        self.rules_class = None
        self.builder_rules = {}
        self.base_inputs = [
            'partition_list',
            'measure_list',
        ]
        self.base_defaults = {
            'logical_comparison': 'AND',
        }
        self.auto_sets = {
            'transform_id': self._create_id,
            'transform_type': self._get_transform_type,
        }

    def _gen_auto_sets(self, outer_keys: dict):
        for key, func in self.auto_sets.items():
            outer_keys[key] = func()
        return outer_keys

    def _create_id(self):
        return str(uuid.uuid1())


    def _get_transform_type(self):
        return self.transform_type

    def _get_transform_name(self, transform):
        return self.builder_rules[transform]['function_name']

    def _sort_keys(self, inputs: dict):
        self._validate_base_inputs(inputs)
        outer_keys = {}
        for k, v in inputs.items():
            if k in self.base_inputs:
                outer_keys[k] = v
        for k,v in self.base_defaults.items():
            if k in inputs.keys():
                outer_keys[k] = inputs[k]
            else:
                outer_keys[k] = v
        for k, v in outer_keys.items():
            inputs.pop(k)
        return outer_keys, inputs

    def _validate_base_inputs(self, inputs: dict):
        missing_inputs = []
        for i in self.base_inputs:
            if i not in inputs.keys():
                missing_inputs.append(i)
        if len(missing_inputs):
            raise ValueError(f"Missing required inputs: {missing_inputs}")

    def _validate_params(self, params: dict, allowed_params: list):
        for k in params.keys():
            if k not in allowed_params:
                params.pop(k)
        return params

    def _build_param_dict(self, function_name, params):
        return build_param_dict(function_name, params.items())

    def build_rules_dict(self, transform, inputs):
        function_name = self._get_transform_name(transform)
        sorted_keys = self._sort_keys(inputs)
        outer_keys = sorted_keys[0]
        params = self._validate_params(sorted_keys[1], self.builder_rules[function_name]['params'])
        outer_keys['param_dict'] = self._build_param_dict(function_name, params)
        outer_keys['transform_name'] = function_name
        outer_keys = self._gen_auto_sets(outer_keys)

        return self.rules_class(**outer_keys)


class FilterBuilder(TransformRulesBuilder):
    def __init__(self):
        super().__init__()
        self.rules_class = FilterRules
        self.transform_type = 'filter_data'
        self.builder_rules = {
            'butter_lowpass': {
                'function_name': 'ButterLowpass',
                'params': [
                    'order',
                    'nyquist',
                    'filter_name',
                ]
            },
            'window_average': {
                'function_name': 'WindowAverage',
                'params': [
                    'window_len',
                    'filter_name',
                ]
            }
        }


class DParamBuilder(TransformRulesBuilder):
    def __init__(self):
        super().__init__()
        self.rules_class = DParamRules
        self.transform_type = 'derive_param'
        self.builder_rules = {
            'heading': {
                'function_name': 'DeriveHeading',
                'params': [
                    'window_len',
                    'units',
                    'heading_type',
                    'swap_lon_lat',
                ],
                'measure_rules': [
                    'output_name',
                    'spatial_measure'
                ],
                'measure_restrictions': {
                    'spatial_measure': [
                        'geo',
                    ]
                }
            },
            'slope': {
                'function_name': 'DeriveSlope',
                'params': [
                    'window_len',
                ],
                'measure_rules': [
                    'output_name',
                    'rise_measure',
                    'run_measure',
                ],
                'measure_restrictions': {}
            },
            'change': {
                'function_name': 'DeriveChange',
                'params': [
                    'window_len',
                    'angle_change',
                ],
                'measure_rules': [
                    'output_name',
                    'target_measure',
                ],
                'measure_restrictions': {}
            },
            'cumsum': {
                'function_name': 'DeriveCumsum',
                'params': [
                    'offset',
                ],
                'measure_rules': [
                    'output_name',
                    'target_measure',
                ],
                'measure_restrictions': {}
            },
            'distance':  {
                'function_name': 'DeriveDistance',
                'params': [
                    'window_len',
                    'distance_func',
                    'swap_lon_lat',
                ],
                'measure_rules': [
                    'output_name',
                    'spatial_measure'
                ],
                'measure_restrictions': {
                    'spatial_measure': [
                        'geo',
                    ]
                }
            },
            'windowsum': {
                'function_name': 'DeriveWindowSum',
                'params': [
                    'window_len',
                ],
                'measure_rules': [
                    'output_name',
                    'target_measure',
                ],
                'measure_restrictions': {}
            },
            'scaled': {
                'function_name': 'DeriveScaled',
                'params': [
                    'scalar',
                ],
                'measure_rules': [
                    'output_name',
                    'target_measure',
                ],
                'measure_restrictions': {}
            },
            'inbox':  {
                'function_name': 'DeriveInBox',
                'params': [
                    'upper_left_corner',
                    'lower_right_corner',
                ],
                'measure_rules': [
                    'output_name',
                    'spatial_measure'
                ],
                'measure_restrictions': {
                    'spatial_measure': [
                        'geo',
                    ]
                }
            },
        }