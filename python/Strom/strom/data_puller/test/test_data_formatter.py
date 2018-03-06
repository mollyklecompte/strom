import unittest

from strom.data_puller.data_formatter import *
from strom.dstream.dstream import DStream


class TestCSVFormatter(unittest.TestCase):
    def setUp(self):

        template_dstream = DStream()
        template_dstream.add_measure("length", "float")
        template_dstream.add_measure("diameter", "float")
        template_dstream.add_measure("whole_weight", "float")
        template_dstream.add_measure("viscera_weight", "float")
        template_dstream.add_user_id("sex")
        template_dstream.add_field("rings")
        self.template = template_dstream
        self.mapping_list = [(0,["user_ids","sex"]), (1,["measures","length", "val"]), (2,["measures","diameter", "val"]), (4,["measures","whole_weight", "val"]), (6,["measures","viscera_weight", "val"]), (8,["fields","rings"]), (3,["timestamp"])]
        self.csvf = CSVFormatter(self.mapping_list, self.template)

    def test_init(self):
        self.assertEqual(self.csvf.mapping, self.mapping_list)
        self.assertEqual(self.csvf.template, self.template)

    def test_format_record(self):
        raw_record = ['M',0.455,0.365,0.095,0.514,0.2245,0.101,0.15,15]
        formatted_record = self.csvf.format_record(raw_record)
        for key in self.template:
            self.assertIn(key, formatted_record)

        self.assertEqual(formatted_record["user_ids"]["sex"],raw_record[0])
        self.assertEqual(formatted_record["measures"]["length"]["val"],raw_record[1])
        self.assertEqual(formatted_record["fields"]["rings"], raw_record[8])
        self.assertEqual(formatted_record["timestamp"], raw_record[3])

    def test_dict_method(self):
        self.assertEqual(self.template["measures"]["viscera_weight"], get_from_dict(self.template, ["measures","viscera_weight"]))
        set_in_dict(self.template, ["user_ids","sex"], "I")
        self.assertEqual(self.template["user_ids"]["sex"],"I")