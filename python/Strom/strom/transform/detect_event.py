import numpy as np
from abc import ABCMeta, abstractmethod
from .transform import Transformer

class DetectEvent(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    def load_params(self, params):
        self.params["event_params"] = params["event_params"]
        self.params["event_name"] = params["event_name"]

    def get_params(self):
        return self.params

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

class DetectThreshold(DetectEvent):
    def __init__(self):
        super().__init__()
        self.params["event_params"] = {"measure":"measure_name", "threshold_value":"value to compare against",
                                       "comparison_operator":["==", "!=", ">=", "<=", ">", "<"]}

        @staticmethod
        def compare_threshold(data_array, comparison_operator, comparision_val):
            comparisons= {"==":np.equal, "!=":np.not_equal, ">=":np.greater_equal, "<=":np.less_equal, ">":np.greater, "<":np.less}
            cur_comp = comparisons[comparison_operator]
            cur_comp(data_array, comparision_val)