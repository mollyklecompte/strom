import unittest
import json
import time
from copy import deepcopy
from strom.kafka.producer.producer import Producer
from strom.kafka.topics.checker import TopicChecker
from strom.engine.engine import ProcessBStreamThread, EngineConsumer, ConsumerThread, TopicCheckThread, EngineThread, Engine
from strom.utils.stopwatch import Timer
from strom.utils.configer import configer as config
from strom.coordinator.coordinator import Coordinator
from strom.database.mongo_management import MongoManager

demo_data_dir = "demo_data/"
dstreams = json.load(open(demo_data_dir + "demo_trip26.txt"))
template = json.load(open(demo_data_dir + "demo_template.txt"))


class TestProcessBstreamThread(unittest.TestCase):
    def setUp(self):
        self.dstreams = dstreams
        self.template = template
        self.coordinator = Coordinator()
        self.processor = ProcessBStreamThread(self.dstreams, self.coordinator)
        self.mongo = MongoManager()
        self.token = self.template["stream_token"]

    def test_init(self):
        self.assertIsInstance(self.processor, ProcessBStreamThread)
        self.assertIsInstance(self.processor.coordinator, Coordinator)

    def test_run(self):
        self.coordinator.process_template(self.template)
        self.processor.start()
        self.processor.join()
        stored_events = self.mongo.get_all_coll("event", self.token)

        self.assertIn("events", stored_events[0].keys())
        self.assertIn("ninety_degree_turn", stored_events[0]["events"].keys())
        self.assertTrue(len(stored_events[0]["events"]["ninety_degree_turn"]) > 2)


class TestEngineConsumer(unittest.TestCase):
    def setUp(self):
        self.topic = b'test'
        self.url = 'localhost:9092'
        self.buffer = []
        self.producer = Producer(self.url, self.topic)
        self.consumer = EngineConsumer(self.url, self.topic, self.buffer, timeout=5000)
        self.dstreams = dstreams

    def test_consume(self):
        stringified = str(self.dstreams)
        self.producer.produce(stringified.encode())
        self.consumer.consume()
        # print(self.consumer.buffer)

        self.assertEqual(len(self.consumer.buffer), len(self.dstreams))

    def test_update_buffer(self):
        new_buff = [1,2,3]
        self.consumer._update_buffer(new_buff)

        self.assertEqual(self.consumer.buffer, new_buff)


class TestConsumerThread(unittest.TestCase):
    def setUp(self):
        self.topic = b'test2'
        self.url = 'localhost:9092'
        self.buffer = []
        self.dstreams = dstreams
        self.producer = Producer(self.url, self.topic)
        self.consumer_thread = ConsumerThread(self.url, self.topic, self.buffer, timeout=5000)

    def test_run(self):
        stringified = str(self.dstreams)
        self.producer.produce(stringified.encode())
        self.consumer_thread.start()
        self.consumer_thread.join()

        self.assertEqual(len(self.consumer_thread.consumer.buffer), len(self.dstreams))


class TestEngineThread(unittest.TestCase):
    def setUp(self):
        self.topic = b'test3'
        self.url = 'localhost:9092'
        self.dstreams = deepcopy(dstreams)
        for i in self.dstreams:
            i['stream_token'] = "new"
        self.template = deepcopy(template)
        self.template['stream_token'] = "new"
        self.token = "new"
        self.producer = Producer(self.url, self.topic)
        self.coordinator = Coordinator()
        self.mongo = MongoManager()
        self.engine_thread = EngineThread(self.url, self.topic, self.coordinator, consumer_timeout=10000)

    def test_run(self):

        self.coordinator.process_template(self.template)
        stringified = str(self.dstreams)
        self.producer.produce(stringified.encode())

        self.engine_thread.start()


        stored_events = self.mongo.get_all_coll("event", self.token)
        self.producer.produce(stringified.encode())
        time.sleep(3)
        print("events length first")
        print(len(stored_events))
        self.producer.produce(stringified.encode())
        time.sleep(3)
        self.producer.produce(stringified.encode())
        time.sleep(9)


        self.engine_thread.join()
        stored_events2 = self.mongo.get_all_coll("event", self.token)
        print("events length second")
        print(len(stored_events2))
        self.assertTrue(len(stored_events2) >= 2)

class TestTopicCheckThread(unittest.TestCase):
    def setUp(self):
        self.checker = TopicChecker()
        self.checker_thread_no_keep = TopicCheckThread(self.checker, callback=self.callback_dummy)
        self.checker_thread_keep = TopicCheckThread(self.checker, callback=self.callback_dummy())
        self.dummy_topics = []

    def callback_dummy(self):
        self.dummy_topics.append("topic")

    def test_init(self):
        self.assertIsInstance(self.checker, TopicChecker)
        self.assertIsInstance(self.checker_thread_keep, TopicCheckThread)
        self.assertIsInstance(self.checker_thread_no_keep, TopicCheckThread)


class TestEngine(unittest.TestCase):

    def setUp(self):
        demo_data_dir = "demo_data/"
        self.dstreams = json.load(open(demo_data_dir + "demo_trip26.txt"))
        self.producer = Producer(config["kafka_url"], b'test')




if __name__ == "__main__":
    unittest.main()