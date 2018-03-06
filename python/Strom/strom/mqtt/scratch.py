""" Non-blocking Python MQTT client with config """
import paho.mqtt.client as mqtt
from abc import ABC, abstractmethod

__version__ = "0.0.2"
__author__ = "Adrian Agnic"

config = {
    "host": "iot.eclipse.org",
    "port": 1883,
    "keepalive": 30,
    "timeout": 10,
    "data": [{
        "topic": "psuba/tura",
        "payload": "test123"
    }]
}


class MQTTClient(ABC, mqtt.Client):
    # NOTE TODO look at TLS options

    def __init__(self, uid=None, userdata=None, transport="tcp", logger=None, asynch=False, **kws):
        """ use reinitialise() for changing instance properties
        :param uid: str: unique idenitifier for this client
        :param userdata: data to be passed through callbacks
        :param transport: str: 'tcp' or 'websockets'
        :param asynch: bool: flag for non-blocking usage
        """
        self.async=asynch
        super().__init__(
            client_id=uid,
            clean_session=False,
            userdata=userdata,
            protocol=mqtt.MQTTv311,
            transport=transport
        )
        super().enable_logger(logger=logger)
        if self.async:
            super().connect_async(host=kws["host"], port=kws["port"], keepalive=kws["keepalive"])

    def _set_throughput(self, inflight, queued):
        """ set before connecting to broker, if needed
        :param inflight: int: num of msgs allowed part-way through network (default 20)
        :param queued: int: num of outgoing msgs allowed pending in queue (default unlimited)
        """
        super().max_inflight_messages_set(inflight)
        super().max_queued_messages_set(queued)

    def _set_websockets(self, path, headers=None):
        """ set before connecting to broker, if using websocket transport
        :param path: str: mqtt path to use on broker
        :param headers: dict: extra headers to append
        """
        super().ws_set_options(path=path, headers=headers)

    def run(self):
        if self.async:
            pass
        else:
            pass

    @abstractmethod
    def on_message(self):
        pass
