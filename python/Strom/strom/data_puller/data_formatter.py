import operator
from abc import ABCMeta, abstractmethod
from copy import deepcopy
from functools import reduce

__version__ = '0.0.1'
__author__ = 'David Nielsen'


#creating some quick utility functions for setting nested values
def get_from_dict(data_dict, mapList):
    return reduce(operator.getitem, mapList, data_dict)

def set_in_dict(data_dict, mapList, value):
    get_from_dict(data_dict, mapList[:-1])[mapList[-1]] = value

class DataFormatter(object, metaclass=ABCMeta):
    def __init__(self, mapping_list, template):
        """

        """
        super().__init__()
        self.mapping = mapping_list
        self.template = template


    @abstractmethod
    def format_record(self, raw_record):
        """This method will transform a single raw record into a DStream"""
        raise NotImplementedError("subclass must implement this abstract method.")

class CSVFormatter(DataFormatter):
    def __init__(self, mapping_list, template):
        """

        """
        super().__init__(mapping_list, template)

    def format_record(self, raw_record):
        tmp_ds = deepcopy(self.template)
        for map in self.mapping:
            set_in_dict(tmp_ds, map[1], raw_record[map[0]])
        return tmp_ds

