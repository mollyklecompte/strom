import json
import os
import unittest
from multiprocessing import Pipe
from time import sleep

from strom.engine.engine import Engine

demo_data_dir = "demo_data/"
# dstreams_str = open(demo_data_dir + "demo_trip26.txt").readline().rstrip()
dstreams = json.load(open(demo_data_dir + "fifty_custom.txt"))


outfile = 'engine_test_output.txt'

def read_outfile():
    with open(outfile) as f:
        lines = [json.loads(l) for l in f]
    return lines

def remove_outfile():
    os.remove(outfile)


class TestEngineThread(unittest.TestCase):
    def setUp(self):
        self.con1, self.con1b = Pipe()
        self.con2, self.con2b = Pipe()
        self.con3, self.con3b = Pipe()
        self.con4, self.con4b = Pipe()
        self.con5, self.con5b = Pipe()
        self.con6, self.con6b = Pipe()
        self.engine = Engine(self.con1b, processors=2, buffer_max_batch=4, buffer_max_seconds=2, test_mode=True)
        self.engine2 = Engine(self.con2b, processors=2, buffer_max_batch=4, buffer_max_seconds=2,
                             test_mode=True)
        self.engine3 = Engine(self.con3b, processors=2, buffer_max_batch=4, buffer_max_seconds=2,
                             test_mode=True)
        self.engine4 = Engine(self.con4b, processors=2, buffer_roll=1, buffer_max_batch=4, buffer_max_seconds=2, test_mode=True)
        self.engine5 = Engine(self.con5b, processors=2, buffer_roll=1, buffer_max_batch=4,
                              buffer_max_seconds=2, test_mode=True)
        self.engine6 = Engine(self.con6b, processors=2, buffer_roll=1, buffer_max_batch=4,
                              buffer_max_seconds=2, test_mode=True)
        self.test_batch1 = [{"stream_token": "abc123", "message": "hi1"}, {"stream_token": "abc123", "message": "hi2"}, {"stream_token": "abc123", "message": "hi3"}, {"stream_token": "abc123", "message": "hi4"}]
        self.test_batch2 = [{"stream_token": "abc1234", "message": "hello1"}, {"stream_token": "abc1234", "message": "hello2"}, {"stream_token": "abc1234", "message": "hello3"}, {"stream_token": "abc1234", "message": "hello4"}]
        self.test_batch3 = [{"stream_token": "abc123", "message": "hi5"}, {"stream_token": "abc123", "message": "hi6"}, {"stream_token": "abc123", "message": "hi7"}, {"stream_token": "abc123", "message": "hi8"}]
        self.test_batch4 = [{"stream_token": "abc1234", "message": "hello5"}, {"stream_token": "abc1234", "message": "hello6"}, {"stream_token": "abc1234", "message": "hello7"}, {"stream_token": "abc1234", "message": "hello8"}]
        # roll results
        self.test_batch5 = [{"stream_token": "abc123", "message": "hi4"}, {"stream_token": "abc123", "message": "hi5"}, {"stream_token": "abc123", "message": "hi6"}, {"stream_token": "abc123", "message": "hi7"}]
        # send batches
        self.test_batch6 = [{"stream_token": "abc123", "message": "hi9"}, {"stream_token": "abc123", "message": "hi10"}, {"stream_token": "abc123", "message": "hi11"}, {"stream_token": "abc123", "message": "hi12"}]
        self.test_batch7 = [{"stream_token": "abc123", "message": "hi13"}, {"stream_token": "abc123", "message": "hi14"}, {"stream_token": "abc123", "message": "hi15"}, {"stream_token": "abc123", "message": "hi16"}]
        # roll results
        self.test_batch8 = [{"stream_token": "abc123", "message": "hi12"}, {"stream_token": "abc123", "message": "hi13"}, {"stream_token": "abc123", "message": "hi14"}, {"stream_token": "abc123", "message": "hi15"}]
        self.test_batch_mix = [{"stream_token": "abc123", "message": "hi1"}, {"stream_token": "abc1234", "message": "hello1"}, {"stream_token": "abc123", "message": "hi2"}, {"stream_token": "abc1234", "message": "hello2"}, {"stream_token": "abc123", "message": "hi3"}, {"stream_token": "abc1234", "message": "hello3"}, {"stream_token": "abc123", "message": "hi4"}, {"stream_token": "abc1234", "message": "hello4"}]
        self.test_batch_1to4 = self.test_batch1 + self.test_batch2 + self.test_batch3 + self.test_batch4

    def test_buffer1(self):
        if os.path.exists(outfile):
            remove_outfile()
        self.engine.start()

        # test all sent to processor in batches of 4
        for i in self.test_batch_1to4:
            self.con1.send(i)
        sleep(2)
        result = read_outfile()

        self.assertEqual(len(result), 4)
        self.assertIn(self.test_batch1, result)
        self.assertIn(self.test_batch2, result)
        self.assertIn(self.test_batch3, result)
        self.assertIn(self.test_batch4, result)

        remove_outfile()
        self.con1.send("stop_poison_pill")

    def test_buffer2(self):
        # test all sent to processor in batches of 4 - GROUPED BY TOKEN (2 buffs)
        sleep(1)
        if os.path.exists(outfile):
            remove_outfile()
        self.engine2.start()
        sleep(3)
        for i in self.test_batch_mix:
            self.con2.send(i)
        sleep(2)
        result2 = read_outfile()
        print(result2)

        self.assertEqual(len(result2), 2)
        self.assertIn(self.test_batch1, result2)
        self.assertIn(self.test_batch2, result2)

        remove_outfile()
        self.con2.send("stop_poison_pill")
        sleep(2)

    def test_buffer3(self):
        # leftovers
        sleep(1)
        if os.path.exists(outfile):
            remove_outfile()
        self.engine3.start()
        sleep(2)
        for i in self.test_batch1[:2]:
            self.con3.send(i)
        for i in self.test_batch2[:2]:
            self.con3.send(i)
        sleep(2.5)
        result3 = read_outfile()

        self.assertEqual(len(result3), 2)
        for x in result3:
            self.assertEqual(len(x), 2)

        remove_outfile()
        self.con3.send("stop_poison_pill")
        sleep(2)

    def test_buffer4(self):
        # w rolling window
        sleep(1)
        if os.path.exists(outfile):
            remove_outfile()
        self.engine4.start()
        sleep(3)

        for i in self.test_batch1 + self.test_batch3:
            self.con4.send(i)
        sleep(3)
        result = read_outfile()
        for r in result:
            print(r)

        self.assertEqual(len(result), 3)
        self.assertIn(self.test_batch1, result)
        self.assertIn(self.test_batch5, result)
        self.assertIn(self.test_batch3[-2:], result)

        remove_outfile()
        self.con4.send("stop_poison_pill")
        sleep(3)

        # no leftovers that are just buffer roll
    def test_buffer5(self):
        sleep(1)
        if os.path.exists(outfile):
            remove_outfile()
            sleep(2)
        self.engine5.start()
        for i in self.test_batch1:
            self.con5.send(i)
        sleep(2.5)
        result2 = read_outfile()
        self.assertEqual(len(result2), 1)
        self.assertIn(self.test_batch1, result2)
        self.con5.send("stop_poison_pill")
        sleep(2)

    def test_buffer6(self):
        # row resets correctly after leftovers
        sleep(1)
        if os.path.exists(outfile):
            remove_outfile()
            sleep(2)
        self.engine6.start()
        for i in self.test_batch1:
            self.con6.send(i)
        for i in self.test_batch3:
            self.con6.send(i)
        sleep(2.1)
        for i in self.test_batch6:
            self.con6.send(i)
        for i in self.test_batch7:
            self.con6.send(i)
        sleep(4)
        result4 = read_outfile()
        for i in result4:
            print(i)

        self.assertEqual(len(result4), 6)
        self.assertIn(self.test_batch1, result4)
        self.assertIn(self.test_batch5, result4)
        self.assertIn(self.test_batch3[-2:], result4)
        self.assertIn(self.test_batch6, result4)
        self.assertIn(self.test_batch8, result4)
        self.assertIn(self.test_batch7[-2:], result4)

        remove_outfile()
        self.con6.send("stop_poison_pill")






