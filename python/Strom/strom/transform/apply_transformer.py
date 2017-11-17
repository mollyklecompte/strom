import importlib
import numpy as np
"""Methods for building an applying Transforms to data"""

def map_to_measure(transform_output_dict):
    output_dict = {}
    for key, val in transform_output_dict.items():
        output_dict[key] = {"val": transform_output_dict[key].tolist(), "dtype": str(transform_output_dict[key].dtype)}

def load_transform(func_type, func_name):
    """Function to load the correct function from name string"""

    return func_name

def apply_transformation(param_dict, bstream):
    if "func_name" in param_dict and "func_type" in param_dict:
        transformer = load_transform(param_dict["func_name"], param_dict["func_name"])
    else:
        raise KeyError("No func_name specified")

    if "measures" in param_dict.keys():
        for measure_name in param_dict["measures"]
        transformer.LOAD FROM BSTREAM
    else:
        raise KeyError("No measures to transform")

    transformer.load_params(param_dict)


    return transformer.transform_data()

#aram_dict: func_type, func_name, measures,
#ransformer needs in param_dict: func_params, measure_rules