from .filter_data import *
from .derive_param import *
from .detect_event import *
from strom.utils.logger.logger import logger

"""Methods for building an applying Transforms to data"""

def map_to_measure(transform_output_dict):
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
    logger.debug("Selecting  filter %s" % (func_name))
    if func_name == "ButterLowpass":
        func = ButterLowpass()
    elif func_name == "WindowAverage":
        func = WindowAverage()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_dparam(func_name):
    logger.debug("Deriving %s" % (func_name))
    if func_name == "DeriveSlope":
        func = DeriveSlope()
    elif func_name == "DeriveChange":
        func = DeriveChange()
    elif func_name == "DeriveDistance":
        func = DeriveDistance()
    elif func_name == "DeriveHeading":
        func = DeriveHeading()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_detect_event(func_name):
    logger.debug("Finding event %s" % (func_name))
    if func_name == "DetectThreshold":
        func = DetectThreshold()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_transform(func_type, func_name):
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
    logger.debug("applying transform to bstream")
    param_dict["stream_token"] = bstream["stream_token"]
    if "func_name" in param_dict and "func_type" in param_dict:
        transformer = select_transform(param_dict["func_type"], param_dict["func_name"])
    else:
        raise KeyError("func_type, func_name not specified")

    if "measures" in param_dict.keys():
        for measure_name in param_dict["measures"]:
            logger.debug("adding %s" % (measure_name))
            transformer.add_measure(measure_name, bstream["measures"][measure_name])
    if "filter_measures" in param_dict.keys():
        for measure_name in param_dict["filter_measures"]:
            logger.debug("adding filtered measure %s" % (measure_name))
            transformer.add_measure(measure_name, bstream["filter_measures"][measure_name])
    if "derived_measures" in param_dict.keys():
        for measure_name in param_dict["derived_measures"]:
            logger.debug("adding derived measure %s" % (measure_name))
            transformer.add_measure(measure_name, bstream["derived_measures"][measure_name])

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
        return map_to_measure(transformer.transform_data())

