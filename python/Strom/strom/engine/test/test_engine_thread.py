import json
import unittest
from time import sleep

from strom.coordinator.coordinator import Coordinator
from strom.engine.engine import EngineThread

demo_data_dir = "demo_data/"
dstreams_str = open(demo_data_dir + "demo_trip26.txt").readline().rstrip()


class TestEngineThread(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator() # need MongoManager, process_data_async for test case
        self.template = json.load(open(demo_data_dir + "demo_template.txt"))
        self.token = self.template["stream_token"]
        self.dstreams = dstreams_str.encode().decode("utf-8")
        self.dlist = [self.dstreams]

    def test_init_with_processors(self):
        engine_thread = EngineThread(processors=2)
        engine_thread.buffer_record_limit = 50
        engine_thread.buffer_time_limit_s = 2
        engine_thread.start()
        self.assertEqual(len(engine_thread.processors), 2)
        for p in engine_thread.processors:
            self.assertTrue(p.is_alive())

    def test_stop_engine(self):
        engine_thread = EngineThread(processors=2)
        engine_thread.buffer_record_limit = 5
        engine_thread.buffer_time_limit_s = 2
        engine_thread.start()
        sleep(2)
        engine_thread.stop_engine()
        self.assertEqual(len(engine_thread.buffer), 0)
        self.assertEqual(engine_thread.message_q.qsize(), 0)
        for p in engine_thread.processors:
            self.assertFalse(p.is_alive())

    def test_run(self):
        self.coordinator.process_template(self.template)
        engine_thread = EngineThread(processors=2)
        engine_thread.buffer_record_limit = 50
        engine_thread.buffer_time_limit_s = 2
        engine_thread.start()
        engine_thread.buffer.extend(self.dlist)
        engine_thread.buffer.extend(self.dlist)
        sleep(4)

        stored_events = self.coordinator.mongo.get_all_coll("event", self.token)
        self.assertIn("events", stored_events[0].keys())
        self.assertIn("ninety_degree_turn", stored_events[0]["events"].keys())

        engine_thread.stop_engine()



