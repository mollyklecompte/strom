"""
Context Module

This module will define a Context class based off a dict. These dicts will be used to store the state for our data pullers
"""
from abc import ABCMeta

from strom.utils.logger.logger import logger

__version__ = '0.0.1'
__author__ = 'David Nielsen'

class Context(dict, metaclass=ABCMeta):
    def __init__(self, *args, **kwargs):
        logger.debug("Initialize Context")
        self.update(*args, **kwargs)


class DirectoryContext(Context):
    def __init__(self, path, file_type, mapping_list, template, *args, **kwargs):
        self.update(*args, **kwargs)
        super().__init__()
        self["dir"] = path
        self["file_type"] = file_type
        self["mapping_list"] = mapping_list
        self["template"] = template
        self["unread_files"] = []
        self["read_files"] = []
        self["header_lines"] = 0
        self["delimiter"] = None

    def add_file(self, file_name):
        self["unread_files"].append(file_name)

    def set_header_len(self, num_header_lines):
        self["header_lines"] = num_header_lines

    def set_delimiter(self, delimiter):
        self["delimiter"] = delimiter

    def read_one(self):
        next_file = self["unread_files"].pop()
        self["read_files"].append(next_file)
        return  next_file



