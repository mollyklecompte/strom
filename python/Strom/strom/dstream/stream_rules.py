"""Filter class to augment filter dictionary with a func_param_name and dtype to create the stream lookup table"""

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"

class RuleDict(dict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        if "expected_keys" in kwargs:
            self.expected_keys = kwargs["expected_keys"]
        else:
            raise KeyError("no expected keys set")

        bad_keys = []
        for key in self.keys():
            if not key in self.expected_keys:
                bad_keys.append(key)
        for key in bad_keys:
            del self[key]
        for key in self.expected_keys:
            if key not in self:
                self[key] = None
    def get_expected_keys(self):
        return self.expected_keys

class FilterRules(RuleDict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        expected_keys = ["func_type", "func_name","filter_name", "func_params", "measures", "derived_measures",]
        super().__init__(expected_keys=expected_keys)


class DParamRules(RuleDict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        expected_keys = ["func_type", "func_name", "", "func_params", "measure_rules", "measures", "derived_measures", ]
        super().__init__(expected_keys=expected_keys)

class StorageRules(RuleDict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        expected_keys = ["store_raw", "store_filtered", "store_derived"]
        super().__init__(expected_keys=expected_keys)

class IngestionRules(RuleDict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        expected_keys = ["nan_handling", "missing_handling",]
        super().__init__(expected_keys=expected_keys)

class EngineRules(RuleDict):
    def __init__(self, *args, **kwargs):
        self.update(*args, **kwargs)
        expected_keys = ["kafka"]
        super().__init__(expected_keys=expected_keys)