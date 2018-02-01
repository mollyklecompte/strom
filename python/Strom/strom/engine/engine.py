"""
Engine Module

It is an engine, it runs. Spins up Kafka & consumes messages;
spawns processes for the aggregation, transformation and storage of data.

Contains...

- class ConsumerThread:
inits + runs kafka consumer in thread.
- class EngineConsumer:
consumes & buffers messages from kafka
whenever message is consumed
- class EngineThread:
inits + runs ConsumerThread, manages buffer, moves data from buffer to processing queue
- class Engine:
engine entrypoint, inits + starts EngineThread(s)
- class Processor:
loads json from data messages to python, runs data transformation + storage process

UNUSED/OLD CLASSES
- class ProcessBstreamThread:
runs coordinator's process_data method on data chunk in thread
-
"""

import numpy as np
from copy import deepcopy
from multiprocessing import Process, Pipe, JoinableQueue
from threading import Thread
from time import time
from .buffer import Buffer
from strom.coordinator.coordinator import Coordinator
from strom.kafka.consumer.consumer import Consumer
from strom.kafka.topics.checker import TopicChecker
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class EngineConsumer(Consumer):
    """
    Based off `Consumer`, from pykafka simple consumer.
    Instantiated by `ConsumerThread`

    Consumes messages & adds them to buffer
    """

    def __init__(self, url, topic, buffer, timeout=-1):
        """
        Initializes EngineConsumer with kafka connection info & buffer passed from EngineThread.
        :param url: kafka url
        :type url: string
        :param topic: kafka topic
        :type topic: string
        :param buffer: buffer from EngineThread
        :type buffer: list
        :param timeout: optional timeout for kafka consumer- defaults to -1 (infinite)
        :type timeout: int
        """
        super().__init__(url, topic, timeout=timeout)
        self.buffer = buffer
        self.topic = topic
        self.topic_name = topic.decode('utf-8')
        logger.info("Initializing EngineConsumer with timeout: {} ms".format(timeout))

    def consume(self):
        """Consumes messages from kafka and appends them to buffer."""
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
        """Resets buffer reference"""
        self.buffer = buffer
        logger.debug("Resetting Engine Consumer buffer")


class ConsumerThread(Thread):
    """
    Based off `threading.Thread`.
    Instantiated by `EngineThread`

    Runs EngineConsumer in thread.
    """

    def __init__(self, url, topic, buffer, timeout=-1):
        """
        Initializes ConsumerThread with EngineConsumer, passing kafka connection info.
        :param url: kafka url
        :type url: string
        :param topic: kafka topic
        :type topic: string
        :param buffer: buffer to pass to EngineConsumer
        :type buffer: list
        :param timeout: kafka consumer timeout
        :type timeout: int
        """
        super().__init__()
        self.consumer = EngineConsumer(url, topic, buffer, timeout=timeout)
        self.consumer_running = None
        logger.debug("Initializing Consumer Thread with timeout: {} ms".format(timeout))

    def run(self):
        """
        Runs thread with kafka consumer.
        """
        self.consumer_running = True
        logger.debug("Starting consumer")
        self.consumer.consume()
        self.consumer_running = False
        if self.consumer_running is False:
            self.consumer.stahp()
            logger.debug("Consumer terminated")


class Processor(Process):
    """
    Based off `multiprocessing.Process`
    Instantiated by `EngineThread`

    Process is started to aggregate + transform data.
    """

    def __init__(self, queue):
        """
        Initializes Processor with queue from EngineThread.
        :param queue: Queue instance where data will come from.
        :type queue: Queue object
        """
        super().__init__()
        self.daemon = True
        self.q = queue
        self.is_running = None

    def run(self):
        """
        Retrieves list of dstreams with queue, runs process to aggregate + transform dstreams.
        Poison Pill: if item pulled from queue is string, "666_kIlL_thE_pROCess_666",
        while loop will break. Do this intentionally.
        """
        coordinator = Coordinator()
        self.is_running = True
        logger.debug("running json loader")
        while self.is_running:
            queued = self.q.get()
            if queued == "666_kIlL_thE_pROCess_666":
                print("HAIL SATAN")
                # self.is_running = False
                break
            else:
                data_list = [datum for datum in queued]
                for data in data_list:
                    coordinator.process_data_async(data, data[0]["stream_token"])
            self.q.task_done()

class EngineThread(Process):
    """
    Based off `threading.Thread`
    Instantiated in server

    Contains buffer, queue for processors, processors, ConsumerThread.
    """

    def __init__(self, engine_conn, processors=8, buffer_roll=0):
        """
        Initializes with empty buffer & queue,
         set # of processors...
        :param processors: number of processors to start
        :type processors: int
        """
        super().__init__()
        self.pipe_conn = engine_conn
        # self.buffer = Buffer(self.buffer_roll)
        self.message_q = JoinableQueue()
        self.number_of_processors = processors
        self.processors = []

        logger.info("Initializing EngineThread")
        self.run_engine = False
        self.buffer_record_limit = int(config["buffer_record_limit"])
        self.buffer_time_limit_s = float(config["buffer_time_limit_s"])
        self.buffer = np.array([{0: 0}] * (
            self.buffer_record_limit * self.number_of_processors)).reshape(
            self.number_of_processors, self.buffer_record_limit)
        self.buffer_roll = -buffer_roll
        if buffer_roll > 0:
            self.buffer_roll_index = -buffer_roll
        else:
            self.buffer_roll_index = None

    def _init_processors(self):
        """Initializes + starts set number of processors"""
        for n in range(self.number_of_processors):
            processor = Processor(self.message_q)
            processor.start()
            self.processors.append(processor)

    # def run(self):
    #     """
    #
    #     """
    #     old_records = 0
    #     self._init_processors()
    #     self.run_engine = True
    #     while self.run_engine:
    #         st = time()
    #         while (len(self.buffer) - old_records) < self.buffer_record_limit \
    #                 and time() - st < self.buffer_time_limit_s:
    #             received = self.pipe_conn.recv()
    #             if received == "stop_poison_pill":
    #                 self.run_engine = False
    #                 break
    #             else:
    #                 self.buffer.append(received)
    #         if len(self.buffer) > old_records:
    #             logger.info("New batch- buffer length: {}".format(len(self.buffer)))
    #             buff_i = len(self.buffer) - old_records + self.buffer_roll
    #             buffer_data = self.buffer[-buff_i:]
    #             self.message_q.put(buffer_data)
    #             if len(self.buffer) >= 150:
    #                 self.buffer.reset()
    #                 logger.info("Resetting buffer")
    #                 old_records -= old_records
    #             else:
    #                 old_records += len(buffer_data) - self.buffer_roll
    #             logger.debug("Took {} s, queue size is {}".format(
    #                 time() - st, str(self.message_q.qsize())))
    #         else:
    #             logger.info("no messages in buffer")

    def run(self):
        """

        """
        self._init_processors()
        self.run_engine = True

        last_col = self.buffer_record_limit - 1
        last_row = self.number_of_processors - 1
        cur_row = 0
        cur_col = 0

        def put_in_buffer(datum):
            self.buffer[cur_row, cur_col] = datum

        while self.run_engine:
            batch_timer = time()

            while time() - batch_timer < self.buffer_time_limit_s:
                item = self.pipe_conn.recv()
                if item == "stop_poison_pill":
                    self.run_engine = False
                    break

                elif type(item) is dict:
                    if cur_row < last_row:
                        if cur_col < last_col:
                            put_in_buffer(item)
                            cur_col += 1
                        else:
                            put_in_buffer(item)
                            self.message_q.put(self.buffer[cur_row].copy())
                            logger.info("New batch queued")
                            roll_window = self.buffer[cur_row, self.buffer_roll_index:] # index/ None roll
                            cur_row += 1
                            for n in roll_window:
                                for i in range(abs(self.buffer_roll)): # num/zero roll ABS
                                    self.buffer[cur_row, i] = n
                            cur_col -= cur_col + self.buffer_roll # num/zero roll subtract
                            batch_timer += time()
                    else:
                        if cur_col < last_col:
                            put_in_buffer(item)
                            cur_col += 1
                        else:
                            put_in_buffer(item)
                            self.message_q.put(self.buffer[cur_row].copy())
                            roll_window = self.buffer[cur_row, self.buffer_roll_index:] # index/ None roll
                            cur_row -= cur_row
                            for n in roll_window:
                                for i in range(abs(self.buffer_roll)): # num/zero roll ABS
                                    self.buffer[cur_row, i] = n
                            cur_col -= cur_col + self.buffer_roll # num/zero roll
                            batch_timer += time()
                else:
                    raise TypeError("Queued item is not valid dictionary.")
            if cur_col >= abs(self.buffer_roll): # num/zero roll ABS
                self.message_q.put(self.buffer[cur_row, :cur_col].copy())

        logger.info("Terminating Engine Thread")
        self.stop_engine()

    def stop_engine(self):
        self.pipe_conn.close()
        if self.run_engine is True:
            self.run_engine = False
        print("JOINING Q")
        logger.info(self.message_q.qsize())
        self.message_q.join()
        logger.info("Queue joined")
        for p in self.processors:
            logger.info("Putting poison pills in Q")
            self.message_q.put("666_kIlL_thE_pROCess_666")
        logger.info("Poison pills done")
        for p in self.processors:
            p.join()
            logger.info("Engine shutdown- processor joined")
        #logger.info("Engine shutdown- EngineThread joined")


class EngineThreadKafka(Thread):
    """
    Based off `threading.Thread`
    Instantiated by `Engine`

    Contains buffer, queue for processors, processors, ConsumerThread.
    """

    def __init__(self, url, topic, processors=8, consumer_timeout=-1):
        """
        Initializes with empty buffer & queue,
         set # of processors, ConsumerThread instance as attributes.
        :param url: kafka url
        :type url: string
        :param topic: kafka topic
        :type topic: string
        :param processors: number of processors to start
        :type processors: int
        :param consumer_timeout: kafka consumer timeout
        :type consumer_timeout: int
        """
        super().__init__()
        self.buffer = []
        self.url = url
        self.topic = topic
        self.topic_name = topic.decode('utf-8')
        self.message_q = JoinableQueue()
        self.number_of_processors = processors
        self.processors = []
        self.buffer_record_limit = config["buffer_record_limit"]
        self.buffer_time_limit_s = config["buffer_time_limit_s"]
        self.consumer_thread = ConsumerThread(self.url,
                                              self.topic,
                                              self.buffer,
                                              timeout=consumer_timeout
                                              )
        logger.info("Initializing EngineThread for topic {}, timeout: {}".format(self.topic_name,
                                                                                 consumer_timeout))
        self._init_processors()

    def _init_processors(self):
        """Initializes + starts set number of processors"""
        for n in range(self.number_of_processors):
            processor = Processor(self.message_q)
            processor.start()
            self.processors.append(processor)

    def _empty_buffer(self):
        """Empties buffer, sets ConsumerThread instance attribute's buffer reference to same"""
        self.buffer = []
        self.consumer_thread.consumer.update_buffer(self.buffer)
        logger.debug("Emptying buffer")

    def _check_consumer(self):
        """Checks to see if ConsumerThread is running"""
        if self.consumer_thread.consumer_running:
            return True
        else:
            return False

    def run(self):
        """
        Starts running ConsumerThread instance attribute.
        When buffer reaches set size or time limit is reached,
        buffer contents are put in queue to be processed &
        buffer is emptied.
        """
        self.consumer_thread.start()
        logger.info("Starting Consumer Thread")

        while self.consumer_thread.is_alive():
            st = time()
            if len(self.buffer):
                logger.debug("Buffer max reached, exiting inner loop")
                buffer_data = deepcopy(self.buffer)
                self._empty_buffer()
                self.message_q.put(buffer_data)
                logger.debug("Took {} s, queue size is {}".format(
                    time() - st, str(self.message_q.qsize())))

            else:
                logger.debug("No records in buffer to process")

        logger.info("Terminating Engine Thread")


class Engine(object):
    """Engine class"""

    def __init__(self):
        """
        Initializes with kafka url + number of processors read from config,
        TopicChecker instance, and empty lists for
        topics + EngineThread instances attributes.
        """
        self.topics = []
        self.kafka_url = config["kafka_url"]
        self.processors = int(config["processors"])
        self.topic_buddy = TopicChecker(self.kafka_url)
        self.engine_threads = []
        logger.info("Engine initializing")
        logger.debug("Kafka URL: {}".format(self.kafka_url))

    def _add_topics_from_list(self, topics):
        """
        Adds topics from static list.
        :param topics: list of topics
        :type topics: list
        """
        self.topics.extend(topics)
        logger.info("Registered topics: {}".format(self.topics))

    def _add_topics_from_client(self):
        """Adds topic via`TopicChecker` method to list topics from kafka client."""
        topics = self.topic_buddy.list()
        self.topics = [k.decode('utf-8') for k, v in topics.items()]
        logger.info("Registered topics: {}".format(self.topics))

    def _add_topic(self, topic):
        """
        Adds a topic.
        :param topic: the name of a topic
        :type topic: string
        """
        self.topics.append(topic)
        logger.info("Registered topic: {}".format(topic))

    def _topic_in_list(self, topic):
        """
        Checks to see if topic is already registered.
        :param topic: name of a topic
        :type topic: string
        :return: boolean for whether topic is registered
        :rtype: boolean
        """
        if topic in self.topics:
            return True
        else:
            return False

    def _new_engine_thread(self, topic, consumer_timeout=-1):
        """Inits + starts new EngineThread instance"""
        engine_thread = EngineThreadKafka(self.kafka_url,
                                     topic.encode(),
                                     processors=self.processors,
                                     consumer_timeout=consumer_timeout
                                     )
        logger.info("Starting engine thread for topic {}, timeout {}".format(topic,
                                                                             consumer_timeout))
        engine_thread.start()

    def _start_all_engine_threads(self, consumer_timeout=-1):
        """
        Starts engine thread for each registered topic,
        appends each to list of registered engine threads attribute.
        :param consumer_timeout: optional kafka consumer timeout
        :type consumer_timeout: int
        """
        for topic in self.topics:
            engine_thread = EngineThreadKafka(self.kafka_url,
                                         topic.encode(),
                                         processors=self.processors,
                                         consumer_timeout=consumer_timeout
                                         )
            logger.info("Starting engine thread for topic {}, timeout {}".format(topic,
                                                                                 consumer_timeout))
            engine_thread.start()
            self.engine_threads.append(engine_thread)

    def run_from_list(self, topics, consumer_timeout=-1):
        """
        Wrapper method to add topics from list + start engine thread for each.
        :param topics: list of topic names
        :type topics: list of strings
        :param consumer_timeout: optional kafka consumer timeout
        :type consumer_timeout: int
        """
        self._add_topics_from_list(topics)
        self._start_all_engine_threads(consumer_timeout=consumer_timeout)

    def run_from_topic_buddy(self, consumer_timeout=-1):
        """
        Wrapper method to add topics via TopicChecker + start engine thread for each.
        :param consumer_timeout: optional kafka consumer timeout
        :type consumer_timeout: int
        """
        self._add_topics_from_client()
        self._start_all_engine_threads(consumer_timeout=consumer_timeout)


class ProcessBStreamThread(Thread):
    """
    Creates thread to call coordinator's process_data on batch of data from kafka.
    Replaced with `Process` class for performance better performance.

    NOT USED
    """
    def __init__(self, data):
        """
        Initializes the thread, decoding message
        :param data: message passed from kafka consumer, in bytes
        """
        super().__init__()
        self.data = data
        self.coordinator = Coordinator()

    def run(self):
        stopwatch['processor timer {}'.format(self.name)].start()
        logger.debug("Starting processor thread")
        logger.debug(self.data[0])
        # self.data = [json.loads(data) for data in self.data]
        self.coordinator.process_data_async(self.data, self.data[0]["stream_token"])
        stopwatch['processor_timer {}'.format(self.name)].stop()
        logger.debug("Terminating processor thread")


def main():
    topics = ['load']
    engine = Engine()
    engine.run_from_list(topics)


if __name__ == "__main__":
    main()
