import json
import os
import uuid
from abc import ABCMeta, abstractmethod

import requests

from .data_formatter import CSVFormatter

__version__ = '0.0.1'
__author__ = 'David Nielsen'

class SourceReader(object, metaclass=ABCMeta):
    def __init__(self):
        """

        """
        super().__init__()

    @abstractmethod
    def read_input(self):
        """This method will read the input from the source"""
        raise NotImplementedError("subclass must implement this abstract method.")

    @abstractmethod
    def return_context(self):
        """This method will return the Reader's context for saving"""
        raise NotImplementedError("subclass must implement this abstract method.")



class DirectoryReader(SourceReader):
    def __init__(self, context, queue=None):
        self.context = context
        if len(self.context["unread_files"]) ==  0:
            for file in os.listdir(self.context['dir']):
                if file.endswith(self.context["file_type"]):
                    self.context.add_file(os.path.abspath(self.context["dir"])+"/"+file)
        if queue is not None:
            self.queue = queue


    def return_context(self):
        return self.context

    def read_input(self):
        if self.context["file_type"] == "csv":
            reader = self.read_csv
        while len(self.context["unread_files"]):
            reader(self.context.read_one())


    def read_csv(self, csv_path):
        self.data_formatter = CSVFormatter(self.context["mapping_list"], self.context["template"])
        print(csv_path)
        with open(csv_path, 'r') as csv_reading:
            for ind in range(self.context["header_lines"]):
                csv_reading.readline()

            for line in csv_reading.readlines():
                line = line.rstrip().split(self.context["delimiter"])
                cur_dstream = self.data_formatter.format_record(line)
                for key, val in cur_dstream.items():
                    if type(val) == uuid.UUID:
                        cur_dstream[key] = str(val)
                if self.context["endpoint"] is not None:
                    r = requests.post(self.context["endpoint"], data=json.dumps(cur_dstream))
                elif self.queue is not None:
                    queue.put(cur_dstream)


