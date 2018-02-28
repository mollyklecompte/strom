
from time import time
from threading import Thread
from strom.data_puller.source_reader import DirectoryReader
from strom.utils.logger.logger import logger


__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


puller_rules = {
    "dir": {
        "reader_class": DirectoryReader,
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



class Data_Puller(Thread):
    def __init__(self, template, context, out_q):
        super().__init__()
        self.source_reader =