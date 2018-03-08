import operator
from functools import reduce

from strom.transform.derive_param import *
from strom.transform.filter_data import *


def get_from_dict(data_dict, mapList):
    return reduce(operator.getitem, mapList, data_dict)

def set_in_dict(data_dict, mapList, value):
    get_from_dict(data_dict, mapList[:-1])[mapList[-1]] = value

def is_in_dict(data_dict, mapList):
    try:
        get_from_dict(data_dict, mapList)
        return True
    except KeyError:
        return False

def set_default_val(param_dict):
    for key, val in param_dict.items():
        if isinstance(val, dict):
            param_dict[key] = set_default_val(val)
        elif isinstance(val, tuple):
            param_dict[key] = val[1]
        else:
            raise ValueError("Incorrectly formatted default param_dict. Values must be dict or tuple")
    return param_dict

def get_default_params(function_name):
    transform_dict = {
        "ButterLowpass":ButterLowpass,
        "WindowAverage":WindowAverage,
        "DeriveSlope":DeriveSlope,
        "DeriveChange":DeriveChange,
        "DeriveCumSum":DeriveCumsum,
        "DeriveDistance":DeriveDistance,
        "DeriveHeading":DeriveHeading,
        "DeriveWindowSum":DeriveWindowSum,
        "DeriveScaled":DeriveScaled,
        "DeriveInBox":DeriveInBox,
        }
    try:
        selected_tranform = transform_dict[function_name]
    except KeyError:
        raise KeyError(f"Unsupported transform: {function_name}")

    return selected_tranform(None)

def build_param_dict(function_name, param_tuples=[], set_defaults=False):
    """

    :param function_name: Name of the transform
    :type function_name: str
    :param param_tuples: List of tuples containing list of keys and value, eg [(["measure_rules","output_name"],"scaled_data"),(["measure_rules","target_measure"],"tempurature"),(["func_params","scalar"],1.852)]
    :type param_tuples: list of tuples
    :return: parameter dict with correct values set
    :rtype:
    """
    defaults = get_default_params(function_name)
    if len(param_tuples):
        defaults = set_default_val(defaults)
        for pt in param_tuples:
            if is_in_dict(defaults, pt[0]):
                set_in_dict(defaults, pt[0], pt[1])
            else:
                bad_keys = "\"][\"".join(pt[0])
                raise KeyError(f"Unexpected key [\"{bad_keys}\"]")
    else:
        if set_defaults:
            defaults = set_default_val(defaults)
        return defaults

    return defaults
