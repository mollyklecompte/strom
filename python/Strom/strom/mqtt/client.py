""" Non-blocking Python MQTT client with config """
import json
import paho.mqtt.client as mqtt
import paho.mqtt.publish as pubber

__version__ = "0.0.2"
__author__ = "Adrian Agnic"


config = {
    "host": "iot.eclipse.org",
    "port": 1883,
    "keepalive": 60,
    "timeout": 10,
    "data": {
        "topic": "psuba",
        "qos": 0,
        "messages": [{
            "topic": "psuba",
            "payload": "hello tura",
            "qos": 0,
            "retain": True
        },
        {
            "topic": "psuba",
            "payload": "test123",
            "qos": 0,
            "retain": True
        }
        ]
    }
}

def generate_message(message, **kws):
    return {
        "topic": kws["data"]["topic"],
        "payload": json.dumps(message),
        "qos": kws["data"]["qos"],
        "retain": True
    }

def publish(msg_list=[], payload=None, keep=False, **kws):
    """
    :param msg_list: list: containing dicts w/ fields 'topic', 'payload', 'qos', 'retain'
    :param keep: bool: flag for retaining, used only with single message in list
    """
    print(f"publishing {len(msg_list)} to {kws['data']['topic']}")
    if len(msg_list):
        pubber.multiple(
            msgs=msg_list,
            hostname=kws["host"],
            port=kws["port"],
            keepalive=kws["keepalive"],
            protocol=mqtt.MQTTv311,
            transport="tcp"
        )
    else:
        pubber.single(
            topic=kws["data"]["topic"],
            payload=payload,
            qos=kws["data"]["qos"],
            retain=keep,
            hostname=kws["host"],
            port=kws["port"],
            keepalive=kws["keepalive"],
            protocol=mqtt.MQTTv311,
            transport="tcp"
        )


class MQTTClient(mqtt.Client):

    def __init__(self, uid=None, userdata=config, transport="tcp", logger=None, asynch=False, **kws):
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

    def _generate_config(self, host, port, topic, keepalive=60, timeout=10, qos=0):
        return {
            "host": host,
            "port": port,
            "keepalive": keepalive,
            "timeout": timeout,
            "data": {
                "topic": topic,
                "qos": qos
            }
        }

    def run(self, **kws):
        if self.async:
            print("async")
            super().loop_start()
        else:
            super().connect(host=kws["host"], port=kws["port"], keepalive=kws["keepalive"])
            while True:
                super().loop(0.1)

    def stop_async_loop(self):
        """ must be called when running asynchronously """
        super().loop_stop()

    def on_message(self, client, userdata, msg):
        print(f"MESSAGE | {msg.payload} from {msg.topic}: {msg.qos} {msg.retain}")

    def on_log(self, client, userdata, lvl, buf):
        print(f"LOG | {lvl}: {buf}")

    def on_connect(self, client, userdata, flags, rc):
        print(f"CONNECTION | {flags}\n RESULT: {rc}")
        super().subscribe(userdata["data"]["topic"], userdata["data"]["qos"])

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print(f"SUBSCRIPTION | {mid}: {granted_qos}")


    def __del__(self):
        super().disconnect()
        super().loop_stop()
