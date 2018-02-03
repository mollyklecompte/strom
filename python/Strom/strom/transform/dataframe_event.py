"""
Class for detecting events in measurs.
This is a subclass of the Transformer class that creates Event objects for each event detected in
the input measures.

apply_transformers calls the DetectEvent class on BStreams and stores output list of Events as
BStream["events"]
"""

import numpy as np

from strom.utils.logger.logger import logger
from .event import Event


def create_events(event_inds):
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


def DetectThreshold(data_frame, params):
    logger.debug("staring DetectThreshold")
    if params == None:
        params = {}
        params["event_rules"] = {"measure":"measure_name", "threshold_value":"value to compare against",
                                    "comparison_operator":["==", "!=", ">=", "<=", ">", "<"]
                                    "absolute_compare":False}
        params["event_name"] = "threshold_event"
        return params

    logger.debug("transforming data")
    measure_array = np.array(self.data[self.params["event_rules"]["measure"]]["val"], dtype=float)
    if "absolute_compare" in params["event_rules"]:
        abs_comp = params["event_rules"]["absolute_compare"]
    else:
        abs_comp = False
    event_inds = compare_threshold(measure_array, params["event_rules"]["comparison_operator"], params["event_rules"]["threshold_value"], abs_comp)
    return self.create_events(event_inds)