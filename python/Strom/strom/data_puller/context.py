"""
Context Module

This module will define a Context class based off a dict. These dicts will be used to store the state for our data pullers
"""
import os
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
        if "unread_files" in kwargs:
            self["unread_files"] = kwargs["unread_files"]
        else:
            self["unread_files"] = []
        if "read_files" in kwargs:
            self["read_files"] = kwargs["read_files"]
        else:
            self["read_files"] = []
        if "header_lines" in kwargs:
            self["header_lines"] =kwargs["header_lines"]
        else:
            self["header_lines"] = 0
        if "delimiter" in kwargs:
            self["delimiter"] = kwargs["delimiter"]
        else:
            self["delimiter"] = None
        if "endpoint" in kwargs:
            self["endpoint"] = kwargs["endpoint"]
        else:
            self["endpoint"] = None
        for file in os.listdir(self['dir']):
            if file.endswith(self["file_type"]):
                self["unread_files"].append(os.path.abspath(self["dir"]) + "/" + file)

    def add_file(self, file_name):
        self["unread_files"].append(file_name)

    def set_header_len(self, num_header_lines):
        self["header_lines"] = num_header_lines

    def set_delimiter(self, delimiter):
        self["delimiter"] = delimiter

    def set_endpoint(self, endpoint):
        self["endpoint"] = endpoint

    def read_one(self):
        next_file = self["unread_files"].pop()
        self["read_files"].append(next_file)
        return  next_file

class KafkaContext(Context):
    def __init__(self, topic, offset, zookeeper, data_format, mapping_list, template, *args, **kwargs):
        self.update(*args, **kwargs)
        super().__init__()
        self["topic"] = topic
        self["offset"] = offset
        self["zookeeper"] = zookeeper
        self["mapping_list"] = mapping_list
        self["template"] = template
        self["format"] = data_format
        if "endpoint" in kwargs:
            self["endpoint"] = kwargs["endpoint"]
        else:
            self["endpoint"] = None





