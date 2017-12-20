from abc import ABCMeta, abstractmethod
from copy import deepcopy

import numpy as np
from strom.utils.logger.logger import logger

from .event import Event
from .transform import Transformer


class DetectEvent(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    def load_params(self, params):
        logger.debug("loading event_rules event_name stream_token")
        self.params["event_rules"] = params["event_rules"]
        self.params["event_name"] = params["event_name"]
        self.params["stream_token"] = params["stream_token"]

    def get_params(self):
        return self.params

    def add_timestamp(self, timestamp_list):
        logger.debug("adding timestamp[s]")
        self.data["timestamp"] = {"val":timestamp_list, "dtype":"decimal"}

    def create_events(self, event_inds):
        logger.debug("creating events")
        event_list = []
        for e_ind in event_inds:
            cur_event = Event({"event_name":self.params["event_name"], "event_rules":self.params["event_rules"],"stream_token":self.params["stream_token"]})
            cur_event["event_ind"] = int(e_ind)
            cur_event["timestamp"] = self.data["timestamp"]["val"][e_ind]
            for key, val in self.data.items():
                logger.debug("added %s to event_context" % (key))
                if key != "timestamp":
                    cur_ind = min(e_ind, len(val["val"])-1)
                    cur_event["event_context"][key] = val["val"][cur_ind]
            event_list.append(deepcopy(cur_event))
        return event_list

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

class DetectThreshold(DetectEvent):
    def __init__(self):
        super().__init__()
        self.params["event_rules"] = {"measure":"measure_name", "threshold_value":"value to compare against",
                                       "comparison_operator":["==", "!=", ">=", "<=", ">", "<"], "absolute_compare":False}
        logger.debug("initialized DetectThreshold. Use get_params() to see parameter values")


    @staticmethod
    def compare_threshold(data_array, comparison_operator, comparision_val, absolute_compare=False):
        logger.debug("comparing: %s %d" %(comparison_operator, comparision_val))
        if absolute_compare:
            data_array = np.abs(data_array)
        comparisons= {"==":np.equal, "!=":np.not_equal, ">=":np.greater_equal, "<=":np.less_equal, ">":np.greater, "<":np.less}
        cur_comp = comparisons[comparison_operator]
        match_inds = np.nonzero(cur_comp(np.nan_to_num(data_array), comparision_val))
        return match_inds[0]

    def transform_data(self):
        logger.debug("transforming data")
        measure_array = np.array(self.data[self.params["event_rules"]["measure"]]["val"], dtype=float)
        if "absolute_compare" in self.params["event_rules"]:
            abs_comp = self.params["event_rules"]["absolute_compare"]
        else:
            abs_comp = False
        event_inds = self.compare_threshold(measure_array, self.params["event_rules"]["comparison_operator"], self.params["event_rules"]["threshold_value"], abs_comp)
        return self.create_events(event_inds)