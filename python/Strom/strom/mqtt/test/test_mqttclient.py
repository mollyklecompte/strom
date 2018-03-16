import unittest
from time import sleep

from strom.mqtt.client import MQTTClient, publish, config


class TestMQTTClient(unittest.TestCase):

    def setUp(self):
        self.c = 0
        self.client = MQTTClient(f"testy{self.c}", asynch=True, **config)
        self.c += 1
        self.client.on_message = self.on_message
        self.msg_arr = []
        publish([], f"hello from testy{self.c-1}", **config)


    def on_message(self, c, ud, msg):
        self.msg_arr.append(msg.payload)

    def test_run(self):
        self.client.run(**config)
        sleep(2)
        self.assertEqual(self.msg_arr[0], "hello from testy0")


    def tearDown(self):
        del self.client
