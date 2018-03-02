import paho.mqtt.client as mqtt

__version__ = "0.0.1"
__author__ = "Adrian Agnic"

config = {
    "host": "iot.eclipse.org",
    "port": 1883,
    "keepalive": 999,
    "topic": "psuba",
    "timeout": 10
}


class MQTTClient(mqtt.Client):

    def __init__(self, uid=None):
        super().__init__(client_id=uid,
                        clean_session=True,
                        userdata=None,
                        protocol=mqtt.MQTTv311,
                        transport="tcp")

    def connect(self, remhost, remport, keepalive=1, binding="", async=False):
        if async:
            super().connect_async(host=remhost, port=remport, keepalive=keepalive, bind_address=binding)
        super().connect(host=remhost, port=remport, keepalive=keepalive, bind_address=binding)

    def looper(self, timeout=1, start=False, stop=False, forever=False):
        if start:
            super().loop_start()
        elif stop:
            super().loop_stop()
        elif forever:
            super().loop_forever(retry_first_connection=False)
        else:
            super().loop(timeout=timeout)

    def send(self, topic, payload, qos=0, retain=False):
        super().publish(topic=topic, payload=payload, qos=qos, retain=retain)

    def subscribe(self, topic, qos=0):
        super().subscribe(topic, qos)

    def run(self, **kw):
        self.connect(kw["host"], kw["port"], kw["keepalive"])
        self.subscribe(kw["topic"])
        rc = 0
        while rc == 0:
            self.looper(kw["timeout"])
        return rc

    def on_message(self, client, userdata, message):
        print(f"Received: {message.payload.decode()} on topic: {message.topic}")
