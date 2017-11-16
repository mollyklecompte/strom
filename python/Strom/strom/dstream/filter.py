"""Filter class to augment filter dictionary with a func_param_name and dtype to create the stream lookup table"""

__version__  = "0.1"
__author__ = "Justine <justine@tura.io>"


class Filter(dict):
    def __init__(self):
        self["func_param_name"] = ""
        self["dtype"] = "float(10, 2)"

    def _set_dtype(self, dtype):
        self["dtype"] = dtype

    def _set_func_param_name(self, func_param_name):
        self["func_param_name"] = func_param_name

# def main():
#     filter_obj = Filter()
#     print("filter_obj", filter_obj)
#     filter_obj._set_func_param_name("param_1")
#     filter_obj._set_dtype("varchar(10)")
#     print("filter_obj", filter_obj)
#
# main()
