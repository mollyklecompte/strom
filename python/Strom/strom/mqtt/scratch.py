import paho.mqtt.client as mqtt

__version__ = "0.0.1"
__author__ = "Adrian Agnic"


class MQTTClient(mqtt.Client)

    def __init__(self):
        super().__init__(client_id="veryunique",
                        clean_session=False,
                        userdata=None,
                        protocol=MQTTv311,
                        transport="tcp")

    def connect(self, remhost, remport, keepalive, binding):
        super().connect(remhost, remport, keepalive, binding)

    def disconnect(self):
        super().disconnect()

    def loop(self, timeout):
        pass

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~CALLBACKS
    def set_on_connect(self, func):
        super().on_connect = func

    def set_on_message(self, func):
        super().on_message = func

    def set_on_disconnect(self, func):
        super().on_disconnect = func
