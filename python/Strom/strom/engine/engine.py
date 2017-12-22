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
from copy import deepcopy
from multiprocessing import Process, Queue
from threading import Thread
from time import time

from strom.coordinator.coordinator import Coordinator
from strom.kafka.consumer.consumer import Consumer
from strom.kafka.topics.checker import TopicChecker
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


# class ProcessBStreamThread(Thread):
#     """
#     Creates thread to call coordinator's process_data on batch of data from kafka
#     """
#     def __init__(self, data):
#         """
#         Initializes the thread, decoding message
#         :param data: message passed from kafka consumer, in bytes
#         """
#         super().__init__()
#         self.data = data
#         self.coordinator = Coordinator()
#
#     def run(self):
#         stopwatch['processor timer {}'.format(self.name)].start()
#         logger.debug("Starting processor thread")
#         logger.debug(self.data[0])
#         # self.data = [json.loads(data) for data in self.data]
#         self.coordinator.process_data_async(self.data, self.data[0]["stream_token"])
#         stopwatch['processor_timer {}'.format(self.name)].stop()
#         logger.debug("Terminating processor thread")


class EngineConsumer(Consumer):
    def __init__(self, url, topic, buffer, timeout=-1):
        super().__init__(url, topic, timeout=timeout)
        self.buffer = buffer
        self.topic = topic
        self.topic_name = topic.decode('utf-8')
        logger.info("Initializing EngineConsumer with timeout: {} ms".format(timeout))

    def consume(self):
        self.consumer.start()  # auto-start
        logger.debug("Consuming messages")
        for msg in self.consumer:
            if msg is not None:
                stopwatch['{}_consumer_timer'.format(self.topic_name)].start()
                # logger.fatal(msg.value.decode("utf-8"))
                message = msg.value.decode("utf-8")
                logger.debug("Message consumed: offset {}".format(msg.offset))
                logger.debug("Message type: {}".format(type(message)))
                self.buffer.append(message)
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
        self.consumer_running = True
        logger.debug("Starting consumer")
        self.consumer.consume()
        self.consumer_running = False
        if self.consumer_running is False:
            self.consumer.stahp()
            logger.debug("Consumer terminated")

class Processor(Process):
    def __init__(self, queue):
        super().__init__()
        self.q = queue
        self.coordinator = Coordinator()
        self.is_running = None

    def run(self):
        self.is_running = True
        logger.debug("running json loader")
        while self.is_running:
            queued = self.q.get()
            if queued == "666_kIlL_thE_pROCess_666":
                print("HAIL SATAN")
                self.is_running = False
                break
            else:
                data_list = [json.loads(datum) for datum in queued]
                for data in data_list:
                    self.coordinator.process_data_async(data, data[0]["stream_token"])


class EngineThread(Thread):
    def __init__(self, url, topic, processors=8, consumer_timeout=-1):
        super().__init__()
        self.buffer = []
        self.url = url
        self.topic = topic
        self.topic_name = topic.decode('utf-8')
        self.message_q = Queue()
        self.number_of_processors = processors
        self.processors = []
        self.buffer_record_limit = config["buffer_record_limit"]
        self.buffer_time_limit_s = config["buffer_time_limit_s"]
        #self.processor = Processor(self.child_json_buffer_pipe)
        #self.receiver = ReceiverThread(self.parent_json_buffer_pipe)
        self.consumer_thread = ConsumerThread(self.url, self.topic, self.buffer, timeout=consumer_timeout)
        logger.info("Initializing Engine Thread for topic {} with Consumer timeout: {}".format(self.topic_name, consumer_timeout))
        self._init_processors()
        #self.receiver.start()

    def _init_processors(self):
        for n in range(self.number_of_processors):
            processor = Processor(self.message_q)
            processor.start()
            self.processors.append(processor)

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

        while self.consumer_thread.is_alive():
            st = time()
            if len(self.buffer):
                logger.debug("Buffer max reached, exiting inner loop")
                buffer_data = deepcopy(self.buffer)
                self._empty_buffer()
                self.message_q.put(buffer_data)
                logger.debug("Took {:.5f} seconds and queue size is {}".format(time() - st, str(self.message_q.qsize())))

            else:
                logger.debug("No records in buffer to process")




        logger.info("Terminating Engine Thread")

# class ReceiverThread(Thread):
#     def __init__(self, pipe):
#         super().__init__()
#         self.pipe = pipe
#
#     def run(self):
#         while True:
#             received = self.pipe.recv()
#             json.loads(received)
#             processor = ProcessBStreamThread(received)
#             processor.start()


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
    topics = ['load']
    engine = Engine()
    engine.run_from_list(topics)

if __name__=="__main__":
    main()
