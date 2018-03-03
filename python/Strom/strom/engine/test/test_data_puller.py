import unittest
from strom.engine.data_puller import DataPuller
from strom.data_puller.source_reader import *
from strom.data_puller.context import *
from strom.dstream.dstream import DStream
from queue import Queue
from time import sleep
from threading import Thread


class TestDataPuller(unittest.TestCase):
    def setUp(self):
        template_dstream = DStream()
        template_dstream['data_rules'] = {
            "pull": True,
            "puller": {
                "type": "dir",
                "inputs": {
                    "path": "strom/data_puller/test/",
                    "file_type": "csv",
                    "delimiter": ","
                }
            },
            "mapping_list": [(0,["user_ids","sex"]), (1,["measures","length", "val"]), (2,["measures","diameter", "val"]), (4,["measures","whole_weight", "val"]), (6,["measures","viscera_weight", "val"]), (8,["fields","rings"]), (3,["timestamp"])]
        }
        template_dstream.add_measure("length", "float")
        template_dstream.add_measure("diameter", "float")
        template_dstream.add_measure("whole_weight", "float")
        template_dstream.add_measure("viscera_weight", "float")
        template_dstream.add_user_id("sex")
        template_dstream.add_field("rings")
        self.template = template_dstream
        self.q = Queue()
        self.puller = DataPuller(self.template, self.q)


    def test_init(self):
        puller = DataPuller(self.template, self.q)
        self.assertEqual(puller.puller_type, "dir")
        self.assertIsInstance(puller.context, DirectoryContext)
        self.assertIsInstance(puller.source_reader, DirectoryReader)

    def test_run(self):
        self.puller.start()
        alist = []
        while True:
            x = self.puller.source_reader.queue.get(timeout=5)
            if x is not None:
                alist.append(x)
            else:
                break
        print(alist)
