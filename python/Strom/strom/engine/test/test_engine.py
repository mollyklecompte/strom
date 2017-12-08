import unittest
import json
import time
from copy import deepcopy
from strom.kafka.producer.producer import Producer
from strom.kafka.topics.checker import TopicChecker
from strom.engine.engine import ProcessBStreamThread, EngineConsumer, ConsumerThread, EngineThread, Engine
from strom.utils.stopwatch import Timer
from strom.utils.configer import configer as config
from strom.coordinator.coordinator import Coordinator
from strom.database.mongo_management import MongoManager

demo_data_dir = "demo_data/"
dstreams_str = open(demo_data_dir + "demo_trip26.txt").readline().rstrip()
template = json.load(open(demo_data_dir + "demo_template.txt"))
dstreams = json.load(open(demo_data_dir + "demo_trip26.txt"))


class TestProcessBstreamThread(unittest.TestCase):
    def setUp(self):
        self.dstreams = dstreams
        self.template = template
        self.coordinator = Coordinator()
        self.processor = ProcessBStreamThread(self.dstreams)
        self.mongo = MongoManager()
        self.token = "abc123"

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
        self.dstreams = dstreams_str

    def test_consume(self):
        #stringy = str(self.dstreams).replace("'", r'\"')
        #stringified = '"' + stringy + '"'
        self.producer.produce(self.dstreams.encode())
        self.consumer.consume()
        # print(self.consumer.buffer)

        self.assertEqual(len(self.consumer.buffer), len(dstreams))

    def test_update_buffer(self):
        new_buff = [1,2,3]
        self.consumer.update_buffer(new_buff)

        self.assertEqual(self.consumer.buffer, new_buff)


class TestConsumerThread(unittest.TestCase):
    def setUp(self):
        self.topic = b'test2'
        self.url = 'localhost:9092'
        self.buffer = []
        self.dstreams = dstreams_str
        self.producer = Producer(self.url, self.topic)
        self.consumer_thread = ConsumerThread(self.url, self.topic, self.buffer, timeout=5000)

    def test_run(self):
        #stringy = str(self.dstreams).replace("'", r'\"')
        #stringified = '"'+stringy+'"'
        self.producer.produce(self.dstreams.encode())
        self.consumer_thread.start()
        self.consumer_thread.join()

        print(len(self.consumer_thread.consumer.buffer))
        self.assertTrue(len(self.consumer_thread.consumer.buffer) % len(dstreams) == 0)


class TestEngineThread(unittest.TestCase):
    def setUp(self):
        self.topic = b'test3'
        self.url = 'localhost:9092'
        self.dstreams = str.replace(deepcopy(dstreams_str), "abc123", "new")
        self.template = deepcopy(template)
        self.template['stream_token'] = "new"
        self.token = "new"
        self.producer = Producer(self.url, self.topic)
        self.coordinator = Coordinator()
        self.engine_thread = EngineThread(self.url, self.topic, consumer_timeout=10000)

    def test_run(self):
        self.coordinator.process_template(self.template)
        #stringy = str(self.dstreams).replace("'", r'\"')
        #stringified = '"'+stringy+'"'
        self.producer.produce(self.dstreams.encode())

        self.engine_thread.start()
        self.assertTrue(self.engine_thread.is_alive())


class TestEngine(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator()
        self.dstreams1 = str.replace(deepcopy(dstreams_str), "abc123", "stream1")
        self.template1 = deepcopy(template)
        self.template1['stream_token'] = "stream1"
        self.token1 = "stream1"

        self.dstreams2 = str.replace(deepcopy(dstreams_str), "abc123", "stream2")
        self.template2 = deepcopy(template)
        self.template2['stream_token'] = "stream2"
        self.token2 = "stream2"

        self.producer1 = Producer(config["kafka_url"], b'beeper1')
        self.producer2 = Producer(config["kafka_url"], b'beeper2')
        self.producer3 = Producer(config["kafka_url"], b'stream1')
        self.producer4 = Producer(config["kafka_url"], b'stream2')
        self.engine = Engine()

    def test_engine_init(self):
        self.assertIsInstance(self.engine, Engine)

    def test_add_topic(self):
        self.engine._add_topic('topictest')
        self.assertIn('topictest', self.engine.topics)

    def test_add_topics_from_client(self):
        self.producer1.produce(b'beep')
        self.producer2.produce(b'beep')
        self.engine.topic_buddy._update()
        self.engine._add_topics_from_client()
        self.assertIn('beeper1', self.engine.topics)
        self.assertIn('beeper2', self.engine.topics)

    def test_add_topic_from_list(self):
        topic_list = ['one', 'two', 'three']
        self.engine._add_topics_from_list(topic_list)
        self.assertIn('one', self.engine.topics)
        self.assertIn('two', self.engine.topics)
        self.assertIn('three', self.engine.topics)

    def test_topic_in_list(self):
        self.engine._add_topic('test')
        result = self.engine._topic_in_list('test')
        self.assertTrue(result)

    def test_start_all_engine_threads(self):
        self.coordinator.process_template(self.template1)
        self.coordinator.process_template(self.template2)
        #stringy = str(self.dstreams1).replace("'", r'\"')
        #stringified1 = '"'+stringy+'"'
        self.producer3.produce(self.dstreams1.encode())
        #stringy2 = str(self.dstreams2).replace("'", r'\"')
        #stringified2 = '"'+stringy2+'"'
        self.producer4.produce(self.dstreams2.encode())
        self.engine.topics = ['stream1', 'stream2']

        self.engine._start_all_engine_threads(consumer_timeout=5000)
        for thread in self.engine.engine_threads:
            self.assertTrue(thread.is_alive())



if __name__ == "__main__":
    unittest.main()