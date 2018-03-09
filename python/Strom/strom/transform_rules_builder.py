import uuid

from strom.build_param_dicts import build_param_dict
from strom.dstream.stream_rules import DParamRules, FilterRules

__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"


class TransformRulesBuilder(object):
    def __init__(self):
        self.transform_type = None
        self.rules_class = None
        self.builder_rules = {}
        self.subkeys = []
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

    def _create_id(self):
        return str(uuid.uuid1())

    def _gen_auto_sets(self, outer_keys: dict):
        for key, func in self.auto_sets.items():
            outer_keys[key] = func()
        return outer_keys

    def _get_allowed_params(self, transform):
        key_sets = [self.builder_rules[transform][b] for b in self.subkeys]
        return [k for set in key_sets for k in set]

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
            if k in inputs.keys():
                inputs.pop(k)
        return outer_keys, inputs

    def _validate_base_inputs(self, inputs: dict):
        missing_inputs = []
        for i in self.base_inputs:
            if i not in inputs.keys():
                missing_inputs.append(i)
        if len(missing_inputs):
            raise ValueError(f"Missing required inputs: {missing_inputs}")

    def _validate_params(self, params: dict, transform: str):
        extras = [k for k in params.keys() if k not in self._get_allowed_params(transform)]
        for k in extras:
            params.pop(k)
        return params

    def _build_param_dict(self, transform, params):
        param_tups = [([x, k], params[k]) for x in self.subkeys for k in self.builder_rules[transform][x] if k in params]
        return build_param_dict(self._get_transform_name(transform), param_tups)

    def build_rules_dict(self, transform: str, inputs: dict):
        sorted_keys = self._sort_keys(inputs)
        outer_keys = sorted_keys[0]
        params = self._validate_params(sorted_keys[1], transform)
        print('PARAMS', params)
        param_dict = self._build_param_dict(transform, params)
        outer_keys['param_dict'] = param_dict
        print('PARAM DICT', param_dict)
        outer_keys = self._gen_auto_sets(outer_keys)

        return self.rules_class(**outer_keys)


class FilterBuilder(TransformRulesBuilder):
    def __init__(self):
        super().__init__()
        self.rules_class = FilterRules
        self.transform_type = 'filter_data'
        self.subkeys = ['params']
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

    def _build_param_dict(self, transform, params):
        param_tups = [([k], params[k]) for x in self.subkeys for k in
                      self.builder_rules[transform][x] if k in params]
        return build_param_dict(self._get_transform_name(transform), param_tups)



class DParamBuilder(TransformRulesBuilder):
    def __init__(self):
        super().__init__()
        self.rules_class = DParamRules
        self.transform_type = 'derive_param'
        self.subkeys = ['func_params', 'measure_rules']
        self.builder_rules = {
            'heading': {
                'function_name': 'DeriveHeading',
                'func_params': [
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
                'func_params': [
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
                'func_params': [
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
                'func_params': [
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
                'func_params': [
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
                'func_params': [
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
                'func_params': [
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
                'func_params': [
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
            'threshold':{
                'function_name': 'DeriveThreshold',
                'func_params': [
                    'threshold_value',
                    'comparison_operator',
                    'absolute_compare',
                ],
                'measure_rules': [
                    'output_name',
                    'target_measure',
                ],
                'measure_restrictions': {}
            },
            'logicalcombination': {
                'function_name': 'DeriveLogicalCombination',
                'func_params': [
                    'combiner',
                ],
                'measure_rules': [
                    'output_name',
                    'first_measure',
                    'second_measure',
                ],
                'measure_restrictions': {}
            },
        }

