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
        """
        Load all the parameters needed for finding events are returning them in the desired format
        :param params: All the parameters needed for the event
        :type params: dict containing the keys "event_rules", "event_name", and "stream_token"
        """
        logger.debug("loading event_rules event_name stream_token")
        self.params["event_rules"] = params["event_rules"]
        self.params["event_name"] = params["event_name"]
        self.params["stream_token"] = params["stream_token"]

    def get_params(self):
        """Method to return function default parameters
          :return: all the stored parameters
          :rtype: dict
        """
        return self.params

    def add_timestamp(self, timestamp_list):
        """
        Add timestamp field to the DetectEvent.data storage because it is stored separately in
        the BStream
        :param timestamp_list: the collection of timestamps to add
        :type timestamp_list: list
        """
        logger.debug("adding timestamp[s]")
        self.data["timestamp"] = {"val":timestamp_list, "dtype":"decimal"}

    def create_events(self, event_inds):
        """
        Function that takes the index of events in the data and uses them to create events
        :param event_inds: indices where the events occur
        :type event_inds: numpy array
        :return: all the events corresponding to event_inds
        :rtype: list of event dicts
        """
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
        """
        Creates a new DetectThreshold object with default parameters. Use get_params() for
        parameter description.
        This event compares the target measure to a threshold value using a binary operator. All
        points where the relation is true are returned as events.
        """
        super().__init__()
        self.params["event_rules"] = {"measure":"measure_name", "threshold_value":"value to compare against",
                                       "comparison_operator":["==", "!=", ">=", "<=", ">", "<"], "absolute_compare":False}
        logger.debug("initialized DetectThreshold. Use get_params() to see parameter values")


    @staticmethod
    def compare_threshold(data_array, comparison_operator, comparision_val, absolute_compare=False):
        """
        Fucntion for comparing an array to a values with a binary operator
        :param data_array: input data
        :type data_array: numpy array
        :param comparison_operator: string representation of the binary operator for comparison
        :type comparison_operator: str
        :param comparision_val: The value to be compared against
        :type comparision_val: float
        :param absolute_compare: specifying whether to compare raw value or absolute value
        :type absolute_compare: Boolean
        :return: the indices where the binary operator is true
        :rtype: numpy array
        """
        logger.debug("comparing: %s %d" %(comparison_operator, comparision_val))
        if absolute_compare:
            data_array = np.abs(data_array)
        comparisons= {"==":np.equal, "!=":np.not_equal, ">=":np.greater_equal, "<=":np.less_equal, ">":np.greater, "<":np.less}
        cur_comp = comparisons[comparison_operator]
        match_inds = np.nonzero(cur_comp(np.nan_to_num(data_array), comparision_val))
        return match_inds[0]

    def transform_data(self):
        """
        Finds and returns all events where the threshold comparision is True
        :return: All the found events
        :rtype: list of event dicts
        """
        logger.debug("transforming data")
        measure_array = np.array(self.data[self.params["event_rules"]["measure"]]["val"], dtype=float)
        if "absolute_compare" in self.params["event_rules"]:
            abs_comp = self.params["event_rules"]["absolute_compare"]
        else:
            abs_comp = False
        event_inds = self.compare_threshold(measure_array, self.params["event_rules"]["comparison_operator"], self.params["event_rules"]["threshold_value"], abs_comp)
        return self.create_events(event_inds)