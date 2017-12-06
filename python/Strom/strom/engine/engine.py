"""
Engine Module

Contains...

- class ProcessBstreamThread:
based off Thread from python standard threading lib,
takes a bstream dict and a coordinator insatance,
run method overrides parent method to call
coordinator's process_data method on bstream

- class EngineConsumer:
based of Consumer class from kafka package,
consume method method overrides parent method to
initialize + start a ProcessBstreamThread
whenever message is consumed

- class Engine:
takes list of kafka topic strings,
generate...

"""

from threading import Thread
from ast import literal_eval
from copy import deepcopy
from time import time, sleep
from strom.kafka.consumer.consumer import Consumer
from strom.coordinator.coordinator import Coordinator
from strom.utils.configer import configer as config


__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class ProcessBStreamThread(Thread):
    """
    Creates thread to call coordinator's process_data on batch of data from kafka
    """
    def __init__(self, data, coordinator):
        """
        Initializes the thread, decoding message
        :param data: message passed from kafka consumer, in bytes
        :param coordinator: a Coordinator instance, initialized by Engine
        """
        super().__init__()
        self.data = data
        self.coordinator = coordinator

    def run(self):
        print("processor thread!")
        self.coordinator.process_data_sync(self.data, self.data[0]["stream_token"])


class EngineConsumer(Consumer):
    def __init__(self, url, topic, buffer, timeout=-1):
        super().__init__(url, topic, timeout=timeout)
        self.buffer = buffer

    def consume(self):
        self.consumer.start()  # auto-start
        for msg in self.consumer:
            if msg is not None:
                # print(str(msg.value) + ": {}".format(msg.offset))
                self.buffer.extend(literal_eval(msg.value.decode("utf-8")))
                # processor = ProcessBstreamThread(msg.value, self.coordinator)
                # processor.start()

    def _update_buffer(self, buffer):
        self.buffer = buffer


class ConsumerThread(Thread):
    def __init__(self, url, topic, buffer, timeout=-1):
        super().__init__()
        self.consumer = EngineConsumer(url, topic, buffer, timeout=timeout)
        self.consumer_running = None

    def run(self):
        self.consumer_running = True
        self.consumer.consume()
        self.consumer_running = False
        if self.consumer_running is False:
            print('IT IS FUCKING FALSE')

    def _update_buffer(self, buffer):
        self.buffer = buffer


class EngineThread(Thread):
    def __init__(self, url, topic, coordinator, consumer_timeout=-1):
        super().__init__()
        self.coordinator = coordinator
        self.buffer = []
        self.url = url
        self.topic = topic
        self.consumer_thread = ConsumerThread(self.url, self.topic, self.buffer, timeout=consumer_timeout)

    def _empty_buffer(self):
        self.buffer = []
        self.consumer_thread.consumer._update_buffer(self.buffer)

    def _check_consumer(self):
        if self.consumer_thread.consumer_running:
            return True
        else:
            return False

    def run(self):
        self.consumer_thread.start()
        timer = time()

        while self.consumer_thread.is_alive():
            print("Starting outer while")
            while len(self.buffer) < 300 and time() - timer < 10:
                pass
            print("starting process")
            if len(self.buffer):
                buffer_data = deepcopy(self.buffer)
                self._empty_buffer()
                processor = ProcessBStreamThread(buffer_data, self.coordinator)
                processor.start()
            timer = time()
            print("is consumer running?")
            result = self._check_consumer()
            print(result)



class Engine(object):
    def __init__(self):
        self.coordinator = Coordinator()
        self.topics = []
        self.kafka_url = config["kafka_url"]

    def _add_topics_from_list(self, topics):
        self.topics.extend(topics)

    def _add_topic(self, topic):
        self.topics.append(topic)

    def topic_in_list(self, topic):
        if topic in self.topics:
            return True

    def new_engine_thread(self, topic):
        engine_thread = EngineThread(self.kafka_url, topic.encode(), self.coordinator)
        engine_thread.start()

    def start_all_engine_threads(self):
        for topic in self.topics:
            engine_thread = EngineThread(self.kafka_url, topic.encode(), self.coordinator)
            engine_thread.start()