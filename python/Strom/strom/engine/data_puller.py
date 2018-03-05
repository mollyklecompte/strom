from datetime import datetime
from threading import Thread
from strom.data_puller.source_reader import *
from strom.data_puller.context import *
from strom.utils.logger.logger import logger


__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


puller_rules = {
    "dir": {
        "reader_class": DirectoryReader,
        "context_class": DirectoryContext,
        "required_inputs": [
            "directory_path",
            "file_type",
        ],
        "defaults": {
            "header_lines": 0,
            "delimiter": None,
        }
    },
}

class DataPuller(Thread):
    def __init__(self, template, q):
        super().__init__()
        self.queue = q
        self.template = template
        self.puller_type = self.template['data_rules']['puller']['type']
        self._init_reader()
        self.pulling = False
        self.pulling_start = None

    def _init_reader(self):
        c_class = puller_rules[self.puller_type]['context_class']
        mapping_list = self.template['data_rules']['mapping_list']
        inputs = self.template['data_rules']['puller']['inputs']
        self.context = self._create_context(c_class, mapping_list, self.template, **inputs)
        self._validate_context()
        p_class = puller_rules[self.puller_type]['reader_class']
        self.source_reader = self._create_reader(p_class, self.context, self.queue)
        self._validate_reader()

    def export_context(self):
        return self.source_reader.return_context()

    def run(self):
        self.pulling = True
        self.pulling_start = datetime.now()
        while self.pulling:
            self.source_reader.read_input()
            # if not self.queue.empty():
            #     print(self.queue.qsize())
        logger.info("Quitting puller")
        print("quitting")
        print(self.export_context())

    def stats(self):
        # not used rn
        if self.pulling:
            return {'started': self.pulling_start, 'running_time': datetime.now() - self.pulling_start}

    @staticmethod
    def _create_context(c_class, mapping_list, template, **inputs):
        context = c_class(mapping_list, template, **inputs)
        return context

    @staticmethod
    def _create_reader(reader_class, context, q):
        reader = reader_class(context, q)
        return reader

    def _validate_reader(self):
        if not isinstance(self.source_reader, SourceReader):
            raise TypeError("Invalid source reader")
        else:
            logger.info("Validated source reader")

    def _validate_context(self):
        if not isinstance(self.context, Context):
            raise TypeError("Invalid data puller context")
        else:
            logger.info("Validated data puller context")

