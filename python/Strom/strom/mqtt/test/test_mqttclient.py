import unittest
from time import sleep

from strom.mqtt.client import MQTTClient, publish

conf = {
    "host": "iot.eclipse.org",
    "port": 1883,
    "keepalive": 60,
    "timeout": 10,
    "data": {
        "topic": "psubatests",
        "qos": 0,
        "messages": [{
            "topic": "psubatests",
            "payload": "hello tura",
            "qos": 0,
            "retain": True
        },
        {
            "topic": "psubatests",
            "payload": "test123",
            "qos": 0,
            "retain": True
        }
        ]
    }
}


class TestMQTTClient(unittest.TestCase):

    def setUp(self):
        self.c = 0
        self.client = MQTTClient(uid=f"testy{self.c}", userdata=conf, asynch=True, **conf)
        publish([], f"hello from testy{self.c}", True, **conf)
        self.c += 1
        self.client.on_message = self.on_message
        self.msg_arr = []


    def on_message(self, c, ud, msg):
        print(msg.payload)
        self.msg_arr.append(msg.payload.decode())

    def test_run(self):
        self.client.run(**conf)
        sleep(5)
        self.assertEqual(self.msg_arr[0], f"hello from testy{self.c-1}")


    def tearDown(self):
        del self.client
