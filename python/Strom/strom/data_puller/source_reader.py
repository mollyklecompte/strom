import json
import os
import uuid
from abc import ABCMeta, abstractmethod

import requests

from strom.utils.configer import configer as config
from .context import DirectoryContext
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
    def __init__(self, directory_path, file_type, mapping_list, dstream_template, header_lines=0, delimiter=None):
        self.dir = directory_path
        self.file_type = file_type
        self.context = DirectoryContext(directory_path, file_type, mapping_list, dstream_template)
        self.context.set_header_len(header_lines)
        if delimiter is not None:
            self.context.set_delimiter(delimiter)
            self.delimiter = delimiter
        for file in os.listdir(directory_path):
            if file.endswith(file_type):
                self.context.add_file(os.path.abspath(directory_path)+"/"+file)
        self.endpoint = 'http://{}:{}/api/load'.format(config['server_host'],
                                                   config['server_port'])

    def return_context(self):
        return self.context

    def read_input(self):
        if self.file_type == "csv":
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
                line = line.rstrip().split(self.delimiter)
                cur_dstream = self.data_formatter.format_record(line)
                for key, val in cur_dstream.items():
                    if type(val) == uuid.UUID:
                        cur_dstream[key] = str(val)
                r = requests.post(self.endpoint, data=json.dumps(cur_dstream))


