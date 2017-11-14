import importlib
"""Methods for building an applying Transforms to data"""

def load_transform(func_type, func_name):
    """Function to load the correct function from name string"""

    return func_name

def apply_transformation(param_dict):
    if "func_name" in param_dict and "func_type" in param_dict:
        transformer = load_transform(param_dict["func_name"], param_dict["func_name"])
    else:
        raise KeyError("No func_name specified")

    transformer.load_params(param_dict)

    if "measures" in param_dict.keys():
        transformer.load_measures(param_dict["measures"])
    else:
        raise KeyError("No measures to transform")

    return transformer.transform_data()