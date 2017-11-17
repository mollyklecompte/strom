from .filter_data import *
from .derive_param import *

"""Methods for building an applying Transforms to data"""

def map_to_measure(transform_output_dict):
    output_dict = {}
    for key, val in transform_output_dict.items():
        output_dict[key] = {"val": transform_output_dict[key].tolist(), "dtype": str(transform_output_dict[key].dtype)}

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
    else:
        raise KeyError("No measures to transform")

    transformer.load_params(param_dict)


    return map_to_measure(transformer.transform_data())


#aram_dict: func_type, func_name, measures,
#ransformer needs in param_dict: func_params, measure_rules