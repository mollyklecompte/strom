import unittest

from strom.data_puller.source_reader import *
from strom.dstream.dstream import DStream


class TestDirectoryReader(unittest.TestCase):
    def setUp(self):
        self.dir = "strom/data_puller/test/"
        self.file_type = "csv"
        template_dstream = DStream()
        template_dstream.add_measure("length", "float")
        template_dstream.add_measure("diameter", "float")
        template_dstream.add_measure("whole_weight", "float")
        template_dstream.add_measure("viscera_weight", "float")
        template_dstream.add_user_id("sex")
        template_dstream.add_field("rings")
        self.template = template_dstream
        self.mapping_list = [(0,["user_ids","sex"]), (1,["measures","length", "val"]), (2,["measures","diameter", "val"]), (4,["measures","whole_weight", "val"]), (6,["measures","viscera_weight", "val"]), (8,["fields","rings"]), (3,["timestamp"])]
        self.delimiter = ","
        self.source_reader = DirectoryReader(self.dir, self.file_type, self.mapping_list, self.template, delimiter=self.delimiter)

    def test_init(self):
        self.assertEqual(self.source_reader.dir, self.dir)
        self.assertEqual(self.source_reader.file_type, self.file_type)
        self.assertEqual(self.source_reader.delimiter, self.delimiter)

    def test_return_context(self):
        context = self.source_reader.return_context()
        self.assertIsInstance(context, DirectoryContext)
        self.assertEqual(context["dir"], self.dir)
        self.assertEqual(context["file_type"], self.file_type)
        self.assertEqual(context["mapping_list"], self.mapping_list)
        self.assertEqual(context["template"], self.template)
        self.assertEqual(context["delimiter"], self.delimiter)

    def test_read_input(self):
        self.source_reader.read_input()
        self.assertEqual(2,len(self.source_reader.context["read_files"]))
        self.assertEqual(0, len(self.source_reader.context["unread_files"]))

    def test_read_csv(self):
        csv_path =self.dir+"abalone0.csv"
        self.source_reader.read_csv(csv_path)
