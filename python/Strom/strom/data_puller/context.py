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
    def __init__(self, mapping_list, template, *args, **kwargs):
        logger.debug("Initialize Context")
        self.update(*args, **kwargs)
        self["mapping_list"] = mapping_list
        self["template"] = template

    def set_value(self, key, value):
        self[key] = value


class DirectoryContext(Context):
    def __init__(self, mapping_list, template, *args, **kwargs):
        self.update(*args, **kwargs)
        super().__init__(mapping_list, template, *args, **kwargs)
        self["dir"] = kwargs["path"]
        self["file_type"] = kwargs["file_type"]

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
        if len(self["unread_files"]) == 0:
            for file in os.listdir(self['dir']):
                if file.endswith(self["file_type"]):
                    self["unread_files"].append(os.path.abspath(self["dir"]) + "/" + file)

    def add_file(self, file_name):
        self["unread_files"].append(file_name)

    def set_header_len(self, num_header_lines):
        self.set_value("header_lines", num_header_lines)

    def set_delimiter(self, delimiter):
        self.set_value("delimiter", delimiter)

    def set_endpoint(self, endpoint):
        self.set_value("endpoint", endpoint)

    def read_one(self):
        next_file = self["unread_files"].pop()
        self["read_files"].append(next_file)
        return  next_file

class KafkaContext(Context):
    def __init__(self, mapping_list, template, *args, **kwargs):
        self.update(*args, **kwargs)
        super().__init__(mapping_list, template, *args, **kwargs)
        self["topic"] = kwargs["topic"]
        self["offset"] = kwargs["offset"]
        self["url"] = kwargs["url"]
        self["format"] = kwargs["data_format"]

        if "timeout" in kwargs:
            self["timeout"] = kwargs["timeout"]
        else:
            self["timeout"] = 1

        if "endpoint" in kwargs:
            self["endpoint"] = kwargs["endpoint"]
        else:
            self["endpoint"] = None

    def set_timeout(self, timeout):
        self.set_value("timeout", timeout)

    def set_endpoint(self, endpoint):
        self.set_value("endpoint", endpoint)

class MQTTContext(Context):
    def __init__(self, mapping_list, template, *args, **kwargs):
        self.update(*args, **kwargs)
        super().__init__(mapping_list, template, *args, **kwargs)


