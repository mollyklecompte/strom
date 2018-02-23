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
                logger.warning("non expected key found: %s" % (key))
        for key in bad_keys:
            del self[key]
        for key in self.expected_keys:
            if key not in self:
                logger.info("No value supplied for %s, setting to None" % (key))
                self[key] = None

    def get_expected_keys(self):
        return self.expected_keys

class FilterRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
        Format for each expected key:
        partition_list: list of tuples, tuple contains (column name, value, comparison_operator) for partition rows
        measure_list: list, column names of measures to be filtered
        transform_type: str, "filter_data" is the expected value
        transform_name: str, name of function in filter_data module
        param_dict: dict, parameters for the filter to use
        logical_comparison: str, supported values: "AND" and "OR". Dictates how partition_list is combined.
        """
        self.update(*args, **kwargs)
        expected_keys = ['transform_id', 'partition_list', 'measure_list', 'transform_type', 'transform_name', 'param_dict', 'logical_comparison', ]
        super().__init__(expected_keys=expected_keys)


class DParamRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
        Format for each expected key:
        partition_list: list of tuples, tuple contains (column name, value, comparison_operator) for partition rows
        measure_list: list, column names of measures to be filtered
        transform_type: str, "filter_data" is the expected value
        transform_name: str, name of function in filter_data module
        param_dict: dict, parameters for the filter to use
        logical_comparison: str, supported values: "AND" and "OR". Dictates how partition_list is combined.
        """
        self.update(*args, **kwargs)

        expected_keys = ['transform_id', 'partition_list', 'measure_list', 'transform_type', 'transform_name', 'param_dict', 'logical_comparison', ]
        super().__init__(expected_keys=expected_keys)

class EventRules(RuleDict):
    def __init__(self, *args, **kwargs):
        """
        Format for each expected key:
        partition_list: list of tuples, tuple contains (column name, value, comparison_operator) for partition rows
        measure_list: list, column names of measures to be filtered
        transform_type: str, "filter_data" is the expected value
        transform_name: str, name of function in filter_data module
        param_dict: dict, parameters for the filter to use
        logical_comparison: str, supported values: "AND" and "OR". Dictates how partition_list is combined.
        callback_rules: CallbackRules()
         """
        self.update(*args, **kwargs)
        expected_keys = ['event_id', 'partition_list', 'measure_list', 'transform_type', 'transform_name', 'param_dict', 'logical_comparison', ]
        # REMOVED callback_rules for now since not yet used -molly 2/22/17
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