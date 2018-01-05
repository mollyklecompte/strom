"""
These dictionaries define the format for the rules dicts used in our modules. They are stored in
DStream objects. Not currently in use but they have provided a template for our demo data and
will be used for data validation and integrity.
"""
from strom.utils.logger.logger import logger

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"

class RuleDict(dict):
    def __init__(self, *args, **kwargs):
        logger.debug("Initialize RuleDict")
        self.update(*args, **kwargs)
        if "expected_keys" in kwargs:
            self.expected_keys = kwargs["expected_keys"]
        else:
            raise KeyError("no expected keys set")

        bad_keys = []
        for key in self.keys():
            if not key in self.expected_keys:
                bad_keys.append(key)
                logger.debug("non expected key found: %s" % (key))
        for key in bad_keys:
            del self[key]
        for key in self.expected_keys:
            if key not in self:
                logger.debug("No value supplied for %s, setting to None" % (key))
                self[key] = None

    def get_expected_keys(self):
        return self.expected_keys

class FilterRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
        Format for each expected key:
        func_type: str, "filter_data" is the expected value
        func_name: str, name of function in filter_data module
        filter_name: str, name of output filtered measure
        func_params: dict, parameters for the filter to use, defaults given by filter_data
        measure: list of str, list of the measure names to be filtered
        filter_measures: list of str, list of the filtered measures to be filtered
        derived_measures: list of str, list of the derived measures to be filtered
        """
        self.update(*args, **kwargs)
        expected_keys = ["func_type", "func_name","filter_name", "func_params", "measures", "filter_measures", "derived_measures",]
        super().__init__(expected_keys=expected_keys)


class DParamRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
         Format for each expected key:
         func_type: str, "derive_param" is the expected value
         func_name: str, name of function in derive_param module
         func_params: dict, parameters for the derive_param function to use
         measure_rules: dict, contains "output_name" key, which names the output derived measure
         measure: list of str, list of the measure names to be derived from
         filter_measures: list of str, list of the filtered measures to be derived from
         derived_measures: list of str, list of the derived measures to be derived from
         """
        self.update(*args, **kwargs)
        expected_keys = ["func_type", "func_name", "func_params", "measure_rules", "measures", "filter_measures", "derived_measures", ]
        super().__init__(expected_keys=expected_keys)

class EventRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
         Format for each expected key:
         func_type: str, "detect_event" is the expected value
         func_name: str, name of function in detect_event module
         event_rules: dict, parameters for the detect_event function to use
         measure: list of str, list of the measure names to find events in
         filter_measures: list of str, list of the filtered measures to find events in
         derived_measures: list of str, list of the derived measures to find events in
         event_name: str, name of event
         stream_token: str, dstream identifying stream token
         callback_rules: CallbackRules()
         """
        self.update(*args, **kwargs)
        expected_keys = ["func_type", "func_name", "event_rules", "measures", "filter_measures", "derived_measures", "event_name", "stream_token", "callback_rules"]
        super().__init__(expected_keys=expected_keys)

class CallbackRules(RuleDict):

    def __init__(self, *args, **kwargs):
        """
         Format for each expected key:
         callback_name: str, name of the callback function
         callback_measures: list of str, list of the names of the measures the callback function uses
         callback_filter_measures: list of str, list of the names of the filtered measures the callback function uses
         callback_derived_measures: list of str, list of the names of the derived measures the callback function uses
         """
        self.update(*args, **kwargs)
        expected_keys = ["callback_name", "callback_measures", "callback_filter_measures", "callback_derived_measures"]
        super().__init__(expected_keys=expected_keys)


class StorageRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
         Format for each expected key:
         store_raw: Boolean
         store_filtered: Boolean
         store_derived: Boolean
         """
        self.update(*args, **kwargs)
        expected_keys = ["store_raw", "store_filtered", "store_derived"]
        super().__init__(expected_keys=expected_keys)

class IngestionRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
         Format for each expected key:
         nan_handling: str, method for handling nans in input data
         missing_handling: str, method for handling missing values in input data
         """
        self.update(*args, **kwargs)
        expected_keys = ["nan_handling", "missing_handling",]
        super().__init__(expected_keys=expected_keys)

class EngineRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
         Format for each expected key:
         kafka: Boolean
         """
        self.update(*args, **kwargs)
        expected_keys = ["kafka"]
        super().__init__(expected_keys=expected_keys)