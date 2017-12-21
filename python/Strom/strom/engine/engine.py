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


import json
import ast
from copy import deepcopy
from time import time
from threading import Thread
from multiprocessing import Process, Pipe
from strom.kafka.topics.checker import TopicChecker
from strom.kafka.consumer.consumer import Consumer
from strom.coordinator.coordinator import Coordinator
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch


__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class ProcessBStreamThread(Process):
    """
    Creates thread to call coordinator's process_data on batch of data from kafka
    """
    def __init__(self, data):
        """
        Initializes the thread, decoding message
        :param data: message passed from kafka consumer, in bytes
        """
        super().__init__()
        self.coordinator = Coordinator()
        logger.fatal("init process thread")
        self.data = data

    def run(self):
        stopwatch['processor timer {}'.format(self.name)].start()
        logger.debug("Starting processor thread")
        logger.debug(self.data[0])
        self.coordinator.process_data_async(self.data, self.data[0]["stream_token"])
        stopwatch['processor_timer {}'.format(self.name)].stop()
        logger.debug("Terminating processor thread")

class JSONLoader(Process):
    def __init__(self, pipe):
        super().__init__()
        self.pipe_end = pipe
        self.is_running = None

    def run(self):
        self.is_running = True
        logger.fatal("running json loader")
        while self.is_running:
            piped = self.pipe_end.recv()
            if piped is not None:
                self.pipe_end.send(json.loads(piped))

class EngineConsumer(Consumer):
    def __init__(self, url, topic, buffer, timeout=-1):
        super().__init__(url, topic, timeout=timeout)
        self.buffer = buffer
        #self.json_loader = JSONLoader(self.buffer)
        self.topic = topic
        self.topic_name = topic.decode('utf-8')
        logger.info("Initializing EngineConsumer with timeout: {} ms".format(timeout))
        logger.fatal("INITIALIZING consumer")

    def consume(self):
        self.consumer.start()  # auto-start
        logger.debug("Consuming messages")
        for msg in self.consumer:
            if msg is not None:
                stopwatch['{}_consumer_timer'.format(self.topic_name)].start()
                # logger.fatal(msg.value.decode("utf-8"))
                message = msg.value.decode("utf-8")
                # message["engine_rules"]["kafka"]
                self.buffer.append(message)
                #logger.fatal("Message consumed: offset {}".format(msg.offset))
                logger.debug("Message type: {}".format(type(message)))
                stopwatch['{}_consumer_timer'.format(self.topic_name)].stop()
            else:
                logger.warning("Consumed empty message")

    def update_buffer(self, buffer):
        self.buffer = buffer
        logger.debug("Resetting Engine Consumer buffer")


class ConsumerThread(Thread):
    def __init__(self, url, topic, buffer, timeout=-1):
        super().__init__()
        self.consumer = EngineConsumer(url, topic, buffer, timeout=timeout)
        self.consumer_running = None
        logger.debug("Initializing Consumer Thread with timeout: {} ms".format(timeout))

    def run(self):
        logger.fatal("CONSUMING ITS REAL")
        self.consumer_running = True
        self.consumer.consume()
        self.consumer_running = False
        if self.consumer_running is False:
            self.consumer.json_loader.is_running = False
            self.consumer.stahp()
            logger.debug("Consumer terminated")


class EngineThread(Thread):
    def __init__(self, url, topic, consumer_timeout=-1):
        super().__init__()
        self.buffer = []
        self.url = 'localhost:9092'
        self.topic = topic
        self.topic_name = topic.decode('utf-8')
        self.buffer_record_limit = config["buffer_record_limit"]
        self.buffer_time_limit_s = config["buffer_time_limit_s"]
        #self._init_sub_buffers()
        #self.parent_consumer_pipe, self.child_consumer_pipe = Pipe()
        self.parent_json_buffer_pipe, self.child_json_buffer_pipe = Pipe()
        self.json_loader = JSONLoader(self.child_json_buffer_pipe)
        self.consumer_thread = ConsumerThread(self.url, self.topic, self.buffer, timeout=consumer_timeout)
        logger.info("Initializing Engine Thread for topic {} with Consumer timeout: {}".format(self.topic_name, consumer_timeout))
        self.json_loader.start()
        #logger.debug("Buffer limit params: {} records or {} seconds".format(config["buffer_record_limit"], config["buffer_time_limit_s"]))

    # def _init_sub_buffers(self):
    #     substreams = ['Parham']
    #     for i in substreams:
    #         self.buffer[i] = []

    def _empty_buffer(self):
        self.buffer = []
        self.consumer_thread.consumer.update_buffer(self.buffer)
        logger.debug("Emptying buffer")

    def _check_consumer(self):
        if self.consumer_thread.consumer_running:
            return True
        else:
            return False

    def run(self):
        self.consumer_thread.start()
        logger.info("Starting Consumer Thread")
        timer = time()

        while self.consumer_thread.is_alive():
            # logger.debug("Consumer thread running")
            logger.fatal("IM THE FUCKING ENGINE THREAD")
            self.buffer = self.consumer_thread.consumer.buffer
            while len(self.buffer) < int(self.buffer_record_limit) and time() - timer < int(self.buffer_time_limit_s):
                pass
            if len(self.buffer):
                logger.fatal("Buffer max reached, exiting inner loop")
                buffer_data = deepcopy(self.buffer)
                self._empty_buffer()

                self.parent_json_buffer_pipe.send(buffer_data)
            else:
                logger.warning("No records in buffer to process")
            piped_back = self.parent_json_buffer_pipe.recv()
            if piped_back is not None:
                logger.fatal(piped_back)
                processor = ProcessBStreamThread(piped_back)
                processor.start()
            # timer = time()
            # result = self._check_consumer()
            # logger.debug("Consumer running: {}".format(result))
        logger.info("Terminating Engine Thread")


class Engine(object):
    def __init__(self):
        self.topics = []
        self.kafka_url = config["kafka_url"]
        self.topic_buddy = TopicChecker(self.kafka_url)
        self.engine_threads = []
        logger.info("Engine initializing")
        logger.debug("Kafka URL: {}".format(self.kafka_url))

    def _add_topics_from_list(self, topics):
        self.topics.extend(topics)
        logger.info("Registered topics: {}".format(self.topics))

    def _add_topics_from_client(self):
        topics = self.topic_buddy.list()
        self.topics = [k.decode('utf-8') for k,v in topics.items()]
        logger.info("Registered topics: {}".format(self.topics))


    def _add_topic(self, topic):
        self.topics.append(topic)
        logger.info("Registered topic: {}".format(topic))

    def _topic_in_list(self, topic):
        if topic in self.topics:
            return True
        else:
            return False

    def _new_engine_thread(self, topic, consumer_timeout=-1):
        engine_thread = EngineThread(self.kafka_url, topic.encode(), consumer_timeout=consumer_timeout)
        logger.info("Starting engine thread for topic {} with Consumer timeout {}".format(topic, consumer_timeout))
        engine_thread.start()

    def _start_all_engine_threads(self, consumer_timeout=-1):
        for topic in self.topics:
            engine_thread = EngineThread(self.kafka_url, topic.encode(), consumer_timeout=consumer_timeout)
            logger.info("Starting engine thread for topic {} with Consumer timeout {}".format(topic, consumer_timeout))
            engine_thread.start()
            self.engine_threads.append(engine_thread)


    def run_from_list(self, topics, consumer_timeout=-1):
        self._add_topics_from_list(topics)
        self._start_all_engine_threads(consumer_timeout=consumer_timeout)
        # if listen:
        #    self._listen_for_new_topics(keep_listening=keep_listening)

    def run_from_topic_buddy(self, consumer_timeout=-1):
        self._add_topics_from_client()
        self._start_all_engine_threads(consumer_timeout=consumer_timeout)
        # if listen:
        #    self._listen_for_new_topics(keep_listening=keep_listening)

def main():
    topics = ['Parham']#, 'Molly', 'David', 'Justine', 'Adrian', 'Kody', 'Lucy', 'Lucky', 'Ricky', 'Allison']
    engine = Engine()
    engine.run_from_list(topics)

if __name__=="__main__":
    main()
