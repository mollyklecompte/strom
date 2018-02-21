"""
Context Module

This module will define a Context class based off a dict. These dicts will be used to store the state for our data pullers
"""

__version__ = '0.0.1'
__author__ = 'David Nielsen'

class Context(dict):
    def __init__(self, *args, **kwargs):
        logger.debug("Initialize Context")
        self.update(*args, **kwargs)


class DirectoryContext(Context):
    def __init__(self, path, file_type):
        self.update(*args, **kwargs)
        super().__init__()
        self["dir"] = path
        self["file_type"] = file_type
        self["unread_files"] = [] # use os to list all files of the type in that dir
        self["read_files"]

    def read_one(self):
        next_file = self["unread_files"].pop
        self["read_files"].append(next_file)
        return  next_file



