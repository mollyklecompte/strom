from .filter_data import *
from .derive_param import *

"""Methods for building an applying Transforms to data"""

def map_to_measure(transform_output_dict):
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
    if func_name == "ButterLowpass":
        func = ButterLowpass()
    elif func_name == "WindowAverage":
        func = WindowAverage()
    else:
        raise ValueError("%s not supported" % func_name)
    return func

def select_dparam(func_name):
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

def select_transform(func_type, func_name):
    if func_type == "filter_data":
        func = select_filter(func_name)
    if func_type == "derive_param":
        func = select_dparam(func_name)
    return func

def apply_transformation(param_dict, bstream):
    if "func_name" in param_dict and "func_type" in param_dict:
        transformer = select_transform(param_dict["func_type"], param_dict["func_name"])
    else:
        raise KeyError("func_type, func_name not specified")

    if "measures" in param_dict.keys():
        for measure_name in param_dict["measures"]:
            transformer.add_measure(measure_name, bstream["measures"][measure_name])
    if "filter_measures" in param_dict.keys():
        for measure_name in param_dict["filter_measures"]:
            transformer.add_measure(measure_name, bstream["filter_measures"][measure_name])
    if "derived_measures" in param_dict.keys():
        for measure_name in param_dict["derived_measures"]:
            transformer.add_measure(measure_name, bstream["derived_measures"][measure_name])

    transformer.load_params(param_dict)
    return map_to_measure(transformer.transform_data())

