import unittest
import json
import signal
from time import sleep
from multiprocessing import Pipe
from strom.engine.engine import EngineThread
from strom.coordinator.coordinator import Coordinator

demo_data_dir = "demo_data/"
# dstreams_str = open(demo_data_dir + "demo_trip26.txt").readline().rstrip()
dstreams = json.load(open(demo_data_dir + "demo_trip26.txt"))


class TestEngineThread(unittest.TestCase):
    def setUp(self):
        self.coordinator = Coordinator() # need MongoManager, process_data_async for test case
        self.template = json.load(open(demo_data_dir + "demo_template.txt"))
        self.token = self.template["stream_token"]
        self.dstreams = dstreams
        self.dlist = [self.dstreams]

    # def test_init_with_processors(self):
    #     conn1, conn2 = Pipe()
    #     engine_thread = EngineThread(conn2, processors=2)
    #     engine_thread.buffer_record_limit = 50
    #     engine_thread.buffer_time_limit_s = 2
    #     engine_thread.start()
    #     print("Engine started")
    #     self.assertEqual(len(engine_thread.processors), 2)
    #     for p in engine_thread.processors:
    #         self.assertTrue(p.is_alive())
    #
    #     engine_thread.stop_engine()

    def test_stop_engine(self):
        conn1, conn2 = Pipe()
        engine_thread = EngineThread(conn2, processors=2)
        engine_thread.buffer_record_limit = 5
        engine_thread.buffer_time_limit_s = 2
        engine_thread.start()
        sleep(2)
        conn1.send("stop_poison_pill")
        self.assertEqual(len(engine_thread.buffer), 0)
        self.assertEqual(engine_thread.message_q.qsize(), 0)
        for p in engine_thread.processors:
            self.assertFalse(p.is_alive())
        engine_thread.terminate()
        sleep(1)
        print("exitcode", engine_thread.exitcode)
        self.assertFalse(engine_thread.is_alive())

    def test_run(self):
        self.coordinator.process_template(self.template)
        conn1, conn2 = Pipe()
        engine_thread = EngineThread(conn2, processors=2)
        engine_thread.buffer_record_limit = 50
        engine_thread.buffer_time_limit_s = 2
        engine_thread.start()
        sleep(10)
        for d in self.dlist:
            conn1.send(d)
        sleep(4)

        stored_events = self.coordinator.mongo.get_all_coll("event", self.token)
        self.assertIn("events", stored_events[0].keys())
        self.assertIn("ninety_degree_turn", stored_events[0]["events"].keys())

        conn1.send("stop_poison_pill")
        sleep(4)
        engine_thread.join()
        engine_thread.terminate()
        sleep(1)
        print("exitcode", engine_thread.exitcode)
        self.assertFalse(engine_thread.is_alive())



