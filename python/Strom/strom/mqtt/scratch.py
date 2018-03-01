import paho.mqtt.client as mqtt

__version__ = "0.0.1"
__author__ = "Adrian Agnic"


class MQTTClient(mqtt.Client):

    def __init__(self):
        super().__init__(client_id="veryunique",
                        clean_session=False,
                        userdata=None,
                        protocol="MQTTv311",
                        transport="tcp")

    def connect(self, remhost, remport, keepalive, binding, async=False):
        if async:
            super().connect_async(host=remhost, port=remport, keepalive=keepalive, bind_address=binding)
        super().connect(host=remhost, port=remport, keepalive=keepalive, bind_address=binding)

    def looper(self, timeout=1, start=False, stop=False, forever=False):
        if start:
            super().loop_start()
        elif stop:
            super().loop_stop()
        elif forever:
            super().loop_forever()
        else:
            super().loop(timeout=timeout)

    def send(self, topic, payload, qos, retain=False):
        pass

    def set_on_connect(self, func):
        super().on_connect = func

    def set_on_message(self, func):
        super().on_message = func

    def set_on_disconnect(self, func):
        super().on_disconnect = func
