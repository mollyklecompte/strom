import numpy as np
from copy import deepcopy
from abc import ABCMeta, abstractmethod
from .transform import Transformer
from .event import Event

class DetectEvent(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    def load_params(self, params):
        self.params["event_rules"] = params["event_rules"]
        self.params["event_name"] = params["event_name"]
        self.params["stream_token"] = params["stream_token"]

    def get_params(self):
        return self.params

    def add_timestamp(self, timestamp_list):
        self.data["timestamp"] = {"val":timestamp_list, "dypte":"decimal"}

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

class DetectThreshold(DetectEvent):
    def __init__(self):
        super().__init__()
        self.params["event_rules"] = {"measure":"measure_name", "threshold_value":"value to compare against",
                                       "comparison_operator":["==", "!=", ">=", "<=", ">", "<"]}

    @staticmethod
    def compare_threshold(data_array, comparison_operator, comparision_val):
        comparisons= {"==":np.equal, "!=":np.not_equal, ">=":np.greater_equal, "<=":np.less_equal, ">":np.greater, "<":np.less}
        cur_comp = comparisons[comparison_operator]
        match_inds = np.nonzero(cur_comp(np.nan_to_num(data_array), comparision_val))
        return match_inds[0]

    def create_events(self, event_inds):
        event_list = []
        for e_ind in event_inds:
            cur_event = Event()
            cur_event["event_name"] = self.params["event_name"]
            cur_event["event_rules"] = self.params["event_rules"]
            cur_event["stream_token"] = self.params["stream_token"]
            cur_event["timestamp"] = self.data["timestamp"]["val"][e_ind]
            for key, val in self.data.items():
                if key != "timestamp":
                    cur_event["event_context"][key] = val["val"][e_ind]
            event_list.append(deepcopy(cur_event))
        return event_list


    def transform_data(self):
        measure_array = np.array(self.data[self.params["event_rules"]["measure"]]["val"], dtype=float)
        event_inds = self.compare_threshold(measure_array, self.params["event_rules"]["comparison_operator"], self.params["event_rules"]["threshold_value"])
        return self.create_events(event_inds)