import unittest
from .scratch import MQTTClient, publish, config

class TestMQTTClient(unittest.TestCase):

    def setUp(self):
        self.c = 0
        self.client = MQTTClient(f"testy{self.count}", asynch=True, **config)
        self.c += 1
        self.client.on_message = self.on_message
        self.msg_arr = []
        publish([], f"hello from testy{self.count-1}", **config)


    def on_message(self, c, ud, msg):
        self.msg_arr.append(msg)
        return msg.payload

    def test_run(self):
        pass


    def tearDown(self):
        del self.client
