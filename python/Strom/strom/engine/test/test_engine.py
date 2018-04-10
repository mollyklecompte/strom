import json
import glob
import os
import unittest
from multiprocessing import Pipe
from time import sleep

from strom.engine.engine import Engine
from strom.dstream.dstream import DStream

demo_data_dir = "demo_data/"
# dstreams_str = open(demo_data_dir + "demo_trip26.txt").readline().rstrip()
dstreams = json.load(open(demo_data_dir + "fifty_custom.txt"))


# to KEEP test output files, comment out call to tearDown at end of each test


def read_outfile(outfile):
    with open(outfile) as f:
        lines = [json.loads(l) for l in f]
    return lines

def remove_outfile(outfile):
    os.remove(outfile)


class TestEngineThread(unittest.TestCase):
    def setUp(self):
        self.con1, self.con1b = Pipe()
        self.con2, self.con2b = Pipe()
        self.con3, self.con3b = Pipe()
        self.con4, self.con4b = Pipe()
        self.con5, self.con5b = Pipe()
        self.con6, self.con6b = Pipe()
        self.engine = Engine(self.con1b, processors=2, buffer_max_batch=4, buffer_max_seconds=5, test_mode=True, test_outfile='engine_test_output/engine_test1')
        self.engine2 = Engine(self.con2b, processors=2, buffer_max_batch=4, buffer_max_seconds=5,
                             test_mode=True, test_outfile='engine_test_output/engine_test2')
        self.engine3 = Engine(self.con3b, processors=2, buffer_max_batch=4, buffer_max_seconds=5,
                             test_mode=True, test_outfile='engine_test_output/engine_test3')
        self.engine4 = Engine(self.con4b, processors=2, buffer_roll=1, buffer_max_batch=4, buffer_max_seconds=5, test_mode=True, test_outfile='engine_test_output/engine_test4')
        self.engine5 = Engine(self.con5b, processors=2, buffer_roll=1, buffer_max_batch=4,
                              buffer_max_seconds=5, test_mode=True, test_outfile='engine_test_output/engine_test5')
        self.engine6 = Engine(self.con6b, processors=2, buffer_roll=1, buffer_max_batch=4,
                              buffer_max_seconds=5, test_mode=True, test_outfile='engine_test_output/engine_test6')

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
        self.outfiles = []
        self.abalone_con, self.abalone_conb = Pipe()
        self.abalone = json.load(open(demo_data_dir + "demo_template_dir.txt"))
        self.abalone_temp = DStream()
        self.abalone_temp.load_from_json(self.abalone)
        self.abalone_engine = Engine(self.abalone_conb, processors=2, buffer_max_batch=10, buffer_max_seconds=5,
                                     test_mode=True,
                                     test_outfile='engine_test_output/engine_test_abalone')
    def tearDown(self):
        print("Tear it all down")
        sleep(1)
        for o in self.outfiles:
            print(o)
            try:
                remove_outfile(o)
            except OSError as oserr:
                print(oserr)
        self.outfiles = []

    def test_buffer1(self):
        outfiles = ['engine_test_output/engine_test1_abc123_1.txt', 'engine_test_output/engine_test1_abc123_2.txt','engine_test_output/engine_test1_abc1234_1.txt','engine_test_output/engine_test1_abc1234_2.txt',]
        self.engine.start()
        # sleep(5)
        # test all sent to processor in batches of 4
        for i in self.test_batch_1to4:
            self.con1.send((i, 'load'))
        sleep(5)
        result = []
        for o in outfiles:
            result.extend(read_outfile(o))

        self.assertEqual(len(result), 4)
        self.assertIn(self.test_batch1, result)
        self.assertIn(self.test_batch2, result)
        self.assertIn(self.test_batch3, result)
        self.assertIn(self.test_batch4, result)

        self.outfiles.extend(outfiles)
        self.con1.send("stop_poison_pill")

    def test_buffer2(self):
        # test all sent to processor in batches of 4 - GROUPED BY TOKEN (2 buffs)
        outfiles = ['engine_test_output/engine_test2_abc123_1.txt', 'engine_test_output/engine_test2_abc1234_1.txt' ]
        self.engine2.start()
        sleep(5)
        for i in self.test_batch_mix:
            self.con2.send((i, 'load'))
        sleep(5)
        result2 = []
        for o in outfiles:
            result2.extend(read_outfile(o))

        self.assertEqual(len(result2), 2)
        self.assertIn(self.test_batch1, result2)
        self.assertIn(self.test_batch2, result2)

        self.outfiles.extend(outfiles)
        self.con2.send("stop_poison_pill")

    def test_buffer3(self):
        # leftovers
        outfiles = ['engine_test_output/engine_test3_abc123_1.txt', 'engine_test_output/engine_test3_abc1234_1.txt']
        self.engine3.start()
        sleep(5)
        for i in self.test_batch1[:2]:
            self.con3.send((i, 'load'))
        for i in self.test_batch2[:2]:
            self.con3.send((i, 'load'))
        sleep(7)
        result3 = []
        for o in outfiles:
            result3.extend(read_outfile(o))
        self.assertEqual(len(result3), 2)
        for x in result3:
            self.assertEqual(len(x), 2)

        self.outfiles.extend(outfiles)
        self.con3.send("stop_poison_pill")

    def test_buffer4(self):
        # w rolling window
        outfiles = ['engine_test_output/engine_test4_abc123_1.txt', 'engine_test_output/engine_test4_abc123_2.txt', 'engine_test_output/engine_test4_abc123_3.txt',]
        self.engine4.start()
        sleep(5)

        for i in self.test_batch1 + self.test_batch3:
            self.con4.send((i, 'load'))
        sleep(7)
        result4 = []
        for o in outfiles:
            result4.extend(read_outfile(o))
        # for r in result4:
        #     print(r)

        self.assertEqual(len(result4), 3)
        self.assertIn(self.test_batch1, result4)
        self.assertIn(self.test_batch5, result4)
        self.assertIn(self.test_batch3[-2:], result4)

        self.outfiles.extend(outfiles)
        self.con4.send("stop_poison_pill")

    def test_buffer5(self):
        # no leftovers that are just buffer roll
        outfiles = ['engine_test_output/engine_test5_abc123_1.txt',]
        self.engine5.start()
        sleep(5)
        for i in self.test_batch1:
            self.con5.send((i, 'load'))
        sleep(7)
        result5 = []
        for o in outfiles:
            result5.extend(read_outfile(o))
        self.assertEqual(len(result5), 1)
        self.assertIn(self.test_batch1, result5)

        self.outfiles.extend(outfiles)
        self.con5.send("stop_poison_pill")

    def test_buffer6(self):
        # row resets correctly after leftovers
        outfiles = ['engine_test_output/engine_test6_abc123_1.txt', 'engine_test_output/engine_test6_abc123_2.txt', 'engine_test_output/engine_test6_abc123_3.txt',
                    'engine_test_output/engine_test6_abc123_4.txt', 'engine_test_output/engine_test6_abc123_5.txt',
                    'engine_test_output/engine_test6_abc123_6.txt',]
        self.engine6.start()
        sleep(5)
        for i in self.test_batch1:
            self.con6.send((i, 'load'))
        for i in self.test_batch3:
            self.con6.send((i, 'load'))
        sleep(7)
        for i in self.test_batch6:
            self.con6.send((i, 'load'))
        for i in self.test_batch7:
            self.con6.send((i, 'load'))
        sleep(7)
        result6 = []
        for o in outfiles:
            result6.extend(read_outfile(o))
        # for i in result6:
        #     print(i)

        self.assertEqual(len(result6), 6)
        self.assertIn(self.test_batch1, result6)
        self.assertIn(self.test_batch5, result6)
        self.assertIn(self.test_batch3[-2:], result6)
        self.assertIn(self.test_batch6, result6)
        self.assertIn(self.test_batch8, result6)
        self.assertIn(self.test_batch7[-2:], result6)

        self.outfiles.extend(outfiles)
        self.con6.send("stop_poison_pill")

    def test_new_with_puller(self):
        self.abalone_engine.start()
        sleep(3)
        self.abalone_con.send((self.abalone_temp, 'new'))
        sleep(5)
        outfiles = glob.glob('engine_test_output/engine_test_abalone*')
        result = []
        for o in outfiles:
            result.extend(read_outfile(o))
        self.assertEqual(len(result), 2)
        self.outfiles.extend(outfiles)
        self.abalone_con.send("stop_poison_pill")






