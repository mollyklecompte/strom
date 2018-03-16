import unittest

from strom.data_puller.context import *
from strom.data_puller.source_reader import *
from strom.dstream.dstream import DStream
from strom.kafka.consumer.consumer import Consumer
from strom.kafka.producer.producer import Producer
from strom.mqtt.client import generate_message, config, publish, MQTTPullingClient


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
        self.endpoint = "http://localhost:5000/api/load"
        self.dc = DirectoryContext(self.mapping_list, self.template, path=self.dir, file_type=self.file_type, delimiter=self.delimiter, endpoint=self.endpoint)
        self.source_reader = DirectoryReader(self.dc)

    def test_init(self):
        self.assertEqual(self.source_reader.context["dir"], self.dir)
        self.assertEqual(self.source_reader.context["file_type"], self.file_type)
        self.assertEqual(self.source_reader.context["delimiter"], self.delimiter)

    def test_return_context(self):
        context = self.source_reader.return_context()
        self.assertIsInstance(context, DirectoryContext)
        self.assertEqual(context["dir"], self.dir)
        self.assertEqual(context["file_type"], self.file_type)
        self.assertEqual(context["mapping_list"], self.mapping_list)
        self.assertEqual(context["template"], self.template)
        self.assertEqual(context["delimiter"], self.delimiter)
        self.assertEqual(context["endpoint"], self.endpoint)

    def test_read_input(self):
        self.source_reader.read_input()
        self.assertEqual(2,len(self.source_reader.context["read_files"]))
        self.assertEqual(0, len(self.source_reader.context["unread_files"]))

    def test_read_csv(self):
        csv_path =self.dir+"abalone0.csv"
        self.source_reader.read_csv(csv_path)

class TestMQTTReader(unittest.TestCase):
    def setUp(self):
        template_dstream = DStream()
        template_dstream.add_measure("length", "float")
        template_dstream.add_measure("diameter", "float")
        template_dstream.add_measure("whole_weight", "float")
        template_dstream.add_measure("viscera_weight", "float")
        template_dstream.add_user_id("sex")
        template_dstream.add_field("rings")
        self.template = template_dstream
        self.mapping_list = [(0, ["user_ids", "sex"]), (1, ["measures", "length", "val"]),
                             (2, ["measures", "diameter", "val"]),
                             (4, ["measures", "whole_weight", "val"]),
                             (6, ["measures", "viscera_weight", "val"]), (8, ["fields", "rings"]),
                             (3, ["timestamp"])]
        self.endpoint = "http://localhost:5000/api/load"
        self.data_format = "csv"
        self.uid = "abaloneID"
        self.test_data = "strom/data_puller/test/abalone0.csv"
        config["walltime"] = 10
        self.mqtt_context = MQTTContext(self.mapping_list, self.template, uid=self.uid, data_format=self.data_format, endpoint=self.endpoint, userdata=config)
        self.mqtt_reader = MQTTReader(self.mqtt_context)

    def test_init(self):
        self.assertEqual(self.mqtt_context, self.mqtt_reader.context)
        self.assertIsInstance(self.mqtt_reader.mqtt_client, MQTTPullingClient)

    def test_read_input(self):
        msg_list = []
        ab_file = open(self.test_data)
        for ab_row in ab_file.readlines():
            msg_list.append(generate_message(ab_row.rstrip().split(","), **config))
        ab_file.close()
        publish(msg_list, **config)
        self.mqtt_reader.read_input()

class TestKafkaReader(unittest.TestCase):
    def setUp(self):
        template_dstream = DStream()
        template_dstream.add_measure("length", "float")
        template_dstream.add_measure("diameter", "float")
        template_dstream.add_measure("whole_weight", "float")
        template_dstream.add_measure("viscera_weight", "float")
        template_dstream.add_user_id("sex")
        template_dstream.add_field("rings")
        self.template = template_dstream
        self.mapping_list = [(0, ["user_ids", "sex"]), (1, ["measures", "length", "val"]),
                             (2, ["measures", "diameter", "val"]),
                             (4, ["measures", "whole_weight", "val"]),
                             (6, ["measures", "viscera_weight", "val"]), (8, ["fields", "rings"]),
                             (3, ["timestamp"])]
        self.url = "localhost:9092"
        self.topic = b"data_pulling"
        self.offset = 28
        self.data_format = "csv"
        self.timeout = 1000
        self.test_data = "strom/data_puller/test/abalone0.csv"

        self.kc = KafkaContext(self.mapping_list, self.template, url=self.url, topic=self.topic, offset=self.offset, data_format=self.data_format, timeout=self.timeout)
        self.kafka_reader = KafkaReader(self.kc)

    def test_init(self):
        self.assertEqual(self.kc, self.kafka_reader.context)
        self.assertIsInstance(self.kafka_reader.consumer, Consumer)

    def test_read_input(self):
        kafka_producer = Producer(self.url, self.topic)
        ab_file = open(self.test_data)
        for ab_row in ab_file.readlines():
            kafka_producer.produce(json.dumps(ab_row.rstrip().split(",")).encode())
        ab_file.close()

        self.kafka_reader.read_input()
        kc2 = self.kafka_reader.return_context()
        self.kafka_reader = KafkaReader(kc2)
        ab_file = open(self.test_data)
        for ab_row in ab_file.readlines():
            kafka_producer.produce(json.dumps(ab_row.rstrip().split(",")).encode())
        ab_file.close()
        self.kafka_reader.read_input()


