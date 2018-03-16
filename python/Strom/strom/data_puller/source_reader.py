import json
import os
import uuid
from abc import ABCMeta, abstractmethod

import requests

from strom.kafka.consumer.consumer import Consumer
from strom.mqtt.client import MQTTPullingClient
from .data_formatter import CSVFormatter

__version__ = '0.0.1'
__author__ = 'David Nielsen'

class SourceReader(object, metaclass=ABCMeta):
    def __init__(self, context, queue=None):
        """
        :param context: Context containing all the information necessary for the reader to find and
        pull the data
        :type context: Context class
        :param queue: Optional argument specifying a queue to append the formatted data to
        :type queue: Engine Buffer object
        """
        super().__init__()
        self.context = context
        self.queue = queue

    @abstractmethod
    def read_input(self):
        """This method will read the input from the source"""
        raise NotImplementedError("subclass must implement this abstract method.")

    def return_context(self):
        return self.context

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
                    self.queue.put(cur_dstream)



class DirectoryReader(SourceReader):
    def __init__(self, context, queue=None):
        super().__init__(context, queue)
        if len(self.context["unread_files"]) ==  0:
            for file in os.listdir(self.context['dir']):
                if file.endswith(self.context["file_type"]):
                    self.context.add_file(os.path.abspath(self.context["dir"])+"/"+file)

    def read_input(self):
        if self.context["file_type"] == "csv":
            reader = self.read_csv
        while len(self.context["unread_files"]):
            reader(self.context.read_one())


class KafkaReader(SourceReader):
    def __init__(self, context, queue=None):
        super().__init__(context, queue)
        self.consumer = Consumer(self.context["url"], self.context["topic"], self.context["timeout"])
        offsets = [(val, self.context["offset"]) for val in self.consumer.consumer.partitions.values()]
        self.consumer.consumer.reset_offsets(partition_offsets=offsets)


    def read_input(self):
        if self.context["format"] == "csv":
            formatter = CSVFormatter(self.context["mapping_list"], self.context["template"])
        elif self.context["format"] == "list":
            formatter = CSVFormatter(self.context["mapping_list"], self.context["template"])
        else:
            raise ValueError("Unsupported format")
        self.consumer.consumer.start()
        for msg in self.consumer.consumer:
            self.context["offset"] = msg.offset
            if msg is not None:
                message = msg.value.decode("utf-8")
                print(message, msg.offset)
                cur_dstream = formatter.format_record(message)
                for key, val in cur_dstream.items():
                    if type(val) == uuid.UUID:
                        cur_dstream[key] = str(val)
                if self.context["endpoint"] is not None:
                    r = requests.post(self.context["endpoint"], data=json.dumps(cur_dstream))
                    print(r)
                elif self.queue is not None:
                    self.queue.put(cur_dstream)



class MQTTReader(SourceReader):
    def __init__(self, context, queue=None):
        super().__init__(context, queue)
        self.mqtt_client = MQTTPullingClient(self.context["uid"], self.context["userdata"], self.context["transport"], self.context["logger"], self.context["asynch"],)

    @staticmethod
    def print_payload(msg):
        print(msg.payload)

    def list_payload(self, msg):
        list_payload = json.loads(msg.payload)
        cur_dstream = self.data_formatter.format_record(list_payload)
        for key, val in cur_dstream.items():
            if type(val) == uuid.UUID:
                cur_dstream[key] = str(val)
        print(cur_dstream)
        if self.context["endpoint"] is not None:
            r = requests.post(self.context["endpoint"], data=json.dumps(cur_dstream))
            print(r)
        elif self.queue is not None:
            self.queue.put(cur_dstream)

    def read_input(self):
        if self.context["format"] == "csv" or self.context["format"] == "list" :
            self.data_formatter = CSVFormatter(self.context["mapping_list"],
                                               self.context["template"])
            self.mqtt_client.set_format_function(self.list_payload)

        self.mqtt_client.run(**self.context["userdata"])
