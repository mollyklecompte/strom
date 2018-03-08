"""
Class for detecting events in measurs.
This is a subclass of the Transformer class that creates Event objects for each event detected in
the input measures.

apply_transformers calls the DetectEvent class on BStreams and stores output list of Events as
BStream["events"]
"""

from strom.transform.derive_param import compare_threshold
from strom.utils.logger.logger import logger


#
#
# def create_events(event_times, data_frame, stream_id, event_name):
#     """
#     Function that takes the index of events in the data and uses them to create events
#     :param event_inds: indices where the events occur
#     :type event_inds: numpy array
#     :return: all the events corresponding to event_inds
#     :rtype: list of event dicts
#     """
#     logger.debug("creating events")
#
#     event_dict = {}
#     event_dict["event_name"] = []
#     event_dict["stream_id"] = []
#     event_dict["event_time"] = []
#
#     for e_ind in event_inds:
#


def DetectThreshold(data_frame, params):
    logger.debug("staring DetectThreshold")
    if params == None:
        params = {}
        params["event_rules"] = {
                                    "measure":("name of measure to be thresholded","measure_name", True),
                                    "threshold_value":("value to compare against",0,True),
                                    "comparison_operator":("one of == != >= <= > <", "==",True),
                                    "absolute_compare":("whether to compare against absolute value instead of raw value",False,False)}
        params["event_name"] = ("name of event","threshold_event",True)
        params["stream_id"] = ("stream_token that this event was found in","UUID",True)
        return params

    logger.debug("Finding events")
    measure_array = data_frame[params["event_rules"]["measure"]].values
    if "absolute_compare" in params["event_rules"]:
        abs_comp = params["event_rules"]["absolute_compare"]
    else:
        abs_comp = False
    event_inds = compare_threshold(measure_array, params["event_rules"]["comparison_operator"], params["event_rules"]["threshold_value"], abs_comp)
    logger.debug("found events")
    event_times=data_frame[["timestamp"]][event_inds]
    logger.debug(params["stream_id"])
    logger.debug(params["event_name"])
    event_times["stream_id"] =  params["stream_id"]
    event_times["event_name"] = params["event_name"]
    logger.debug(event_times.to_string())
    return event_times
