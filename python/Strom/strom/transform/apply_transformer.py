from strom.utils.logger.logger import logger

from .derive_param import *
from .detect_event import *
from .filter_data import *

"""Methods for building an applying Transforms to data"""

def map_to_measure(transform_output_dict):
    """
    Convert output of transformer to BStream measure format
    :param transform_output_dict: the output from the transformer
    :type transform_output_dict: dict
    :return: the converted output
    :rtype: dict in BStream measure format
    """
    logger.debug("formatting output as measure")
    return_dict = {}
    for key, val in transform_output_dict.items():
        if len(val.shape) > 1:
            dtype_str = "varchar"
        elif "float" in str(val.dtype):
            dtype_str = "decimal"
        elif "int" in str(val.dtype):
            dtype_str = "int"
        else:
            dtype_str = "varchar"
        return_dict[key] = {"val": val.tolist(), "dtype": dtype_str}
    return return_dict

def select_filter(func_name):
    """
    Function to select filter from filter_data module
    :param func_name: Name of the function to return
    :type func_name: str
    :return: the specified filter
    :rtype: Filter object
    """
    logger.debug("Selecting  filter %s" % (func_name))
    if func_name == "ButterLowpass":
        func = ButterLowpass()
    elif func_name == "WindowAverage":
        func = WindowAverage()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_dparam(func_name):
    """
    Fuction to select derived parameter from derive_param module
    :param func_name: name of the function to return
    :type func_name: str
    :return: the derive parameter object
    :rtype: DeriveParam class object
    """
    logger.debug("Deriving %s" % (func_name))
    if func_name == "DeriveSlope":
        func = DeriveSlope()
    elif func_name == "DeriveChange":
        func = DeriveChange()
    elif func_name == "DeriveCumsum":
        func = DeriveCumsum()
    elif func_name == "DeriveDistance":
        func = DeriveDistance()
    elif func_name == "DeriveHeading":
        func = DeriveHeading()
    elif func_name == "DeriveWindowSum":
        func = DeriveWindowSum()
    elif func_name == "DeriveScaled":
        func = DeriveScaled()
    elif func_name == "DeriveInBox":
        func = DeriveInBox()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_detect_event(func_name):
    """
    Function to select the type of event to find
    :param func_name: name of the event to return
    :type func_name: str
    :return: the specified event detector
    :rtype: DetectEvent class object
    """
    logger.debug("Finding event %s" % (func_name))
    if func_name == "DetectThreshold":
        func = DetectThreshold()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_transform(func_type, func_name):
    """
    Function to return the specified transform object
    :param func_type: type of transform
    :type func_type: str
    :param func_name: name of transform
    :type func_name: str
    :return: the specified transformer
    :rtype: Transform class object
    """
    logger.debug("Selecting %s transform" % (func_type))
    if func_type == "filter_data":
        func = select_filter(func_name)
    elif func_type == "derive_param":
        func = select_dparam(func_name)
    elif func_type == "detect_event":
        func = select_detect_event(func_name)
    else:
        raise ValueError("%s not supported" % func_type)
    return func

def apply_transformation(param_dict, bstream):
    """
    Function that applies a transformer class object to a BStream object to get the desired
    measure or event
    :param param_dict: transform parameter
    :type param_dict: dict
    :param bstream: The bstream to transform
    :type bstream: BSTream class object
    :return: the measure or events specified by param_dict
    :rtype: measure dict or list of event dicts
    """
    logger.debug("applying transform to bstream")
    param_dict["stream_token"] = bstream["stream_token"]
    if "func_name" in param_dict and "func_type" in param_dict:
        transformer = select_transform(param_dict["func_type"], param_dict["func_name"])
    else:
        raise KeyError("func_type, func_name not specified")
    transformer.load_params(param_dict)

    if param_dict["func_type"] == "detect_event":
        logger.debug("This is an event, we must add timestamp and full measurs for context")
        transformer.add_timestamp(bstream["timestamp"])
        transformer.load_measures(bstream["measures"])
        if "filter_measures" in bstream:
            transformer.load_measures(bstream["filter_measures"])
        if "derived_measures" in bstream:
            transformer.load_measures(bstream["derived_measures"])
        return transformer.transform_data()
    else:
        if "measures" in param_dict.keys():
            for measure_name in param_dict["measures"]:
                logger.debug("adding %s" % (measure_name))
                if measure_name == "timestamp":
                    transformer.add_measure(measure_name, {"val": bstream[measure_name], "dtype": "integer"})
                else:
                    transformer.add_measure(measure_name, bstream["measures"][measure_name])
        if "filter_measures" in param_dict.keys():
            for measure_name in param_dict["filter_measures"]:
                logger.debug("adding filtered measure %s" % (measure_name))
                transformer.add_measure(measure_name, bstream["filter_measures"][measure_name])
        if "derived_measures" in param_dict.keys():
            for measure_name in param_dict["derived_measures"]:
                logger.debug("adding derived measure %s" % (measure_name))
                transformer.add_measure(measure_name, bstream["derived_measures"][measure_name])
        return map_to_measure(transformer.transform_data())

