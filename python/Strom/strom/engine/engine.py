"""
Engine Module

It is an engine, it runs. Puts piped data in buffer, spawns processes for the aggregation, transformation and storage of data.

Contains...
- class EngineThread:
manages buffer, moves data from buffer to processing queue
- class Processor:
loads json from data messages to python, runs data transformation + storage process
"""
import os
import json
import numpy as np
from multiprocessing import Process, JoinableQueue
from threading import Thread
from queue import Queue
from time import time
from strom.coordinator.coordinator import Coordinator
from strom.utils.logger.logger import logger

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Processor(Process):
    """
    Based off `multiprocessing.Process`
    Instantiated by `EngineThread`

    Process is started to aggregate + transform data.
    """

    def __init__(self, queue, engine_test_mode):
        """
        Initializes Processor with queue from EngineThread.
        :param queue: Queue instance where data will come from.
        :type queue: Queue object
        """
        super().__init__()
        self.daemon = True
        self.q = queue
        self.is_running = None
        self.test_run = engine_test_mode

    def _write_test_data(self, data, outfile='engine_test_output.txt'):

        if os.path.exists(outfile):
            append_write = 'a'  # append if already exists
        else:
            append_write = 'w'  # make a new file if not

        test_output = open(outfile, append_write)
        test_output.write(data)
        test_output.close()

    def run(self):
        """
        Retrieves list of dstreams with queue, runs process to aggregate + transform dstreams.
        Poison Pill: if item pulled from queue is string, "666_kIlL_thE_pROCess_666",
        while loop will break. Do this intentionally.
        """
        coordinator = Coordinator()
        self.is_running = True
        while self.is_running:
            queued = self.q.get()
            if type(queued) is str:
                if queued == "666_kIlL_thE_pROCess_666":
                    # self.is_running = False
                    break
            else:
                data = queued.tolist()
                if self.test_run:
                    self._write_test_data(f"{json.dumps(data)}\n")
                else:
                    coordinator.process_data(data, data[0]["stream_token"])

            self.q.task_done()



class Engine(Process):
    """
    Based off `threading.Thread`
    Instantiated in server

    Contains buffer, queue for processors, processors, ConsumerThread.
    """

    def __init__(self, engine_conn, processors=4, buffer_roll=0, buffer_max_batch=50, buffer_max_seconds=1, test_mode=False):
        """
        Initializes with empty buffer & queue,
         set # of processors...
        :param processors: number of processors to start
        :type processors: int
        """
        logger.info("Initializing EngineThread")
        super().__init__()
        self.test_run = test_mode
        self.pipe_conn = engine_conn
        self.message_q = JoinableQueue()
        self.number_of_processors = processors
        self.processors = []
        self.run_engine = False
        self.buffer_record_limit = int(buffer_max_batch)
        self.buffer_time_limit_s = float(buffer_max_seconds)
        self.buffers = {}
        self.partition_qs = {}
        self.buffer_workers = {}
        self.buffer_roll = -buffer_roll
        if buffer_roll > 0:
            self.buffer_roll_index = -buffer_roll
        else:
            self.buffer_roll_index = None

    def _init_processors(self):
        """Initializes + starts set number of processors"""
        for n in range(self.number_of_processors):
            processor = Processor(self.message_q, self.test_run)
            processor.start()
            self.processors.append(processor)


    def run(self):
        """
        Sets up numpy array buffer and puts stuff in and gets stuff out
        """
        self._init_processors()
        self.run_engine = True

        while self.run_engine:
            if self.pipe_conn.poll():
                item = self.pipe_conn.recv()
                # branch 2 - stop engine
                if item == "stop_poison_pill":
                    for q in self.partition_qs.keys():
                        self.partition_qs[q].put("stop_buffer_worker")
                    self.run_engine = False
                    break
                # branch 1 - engine running, good data
                elif type(item) is dict:
                    partition_key = item['stream_token']

                    if partition_key not in self.buffers:
                        self.buffers[partition_key] = np.array([{0: 0}] * (self.buffer_record_limit * self.number_of_processors)).reshape(self.number_of_processors, self.buffer_record_limit)
                    if partition_key not in self.partition_qs:
                        self.partition_qs[partition_key] = Queue()
                    if partition_key not in self.buffer_workers:
                        self.buffer_workers[partition_key] = Thread(target=self.run_buffer, args=[partition_key])
                        self.buffer_workers[partition_key].start()
                    self.partition_qs[partition_key].put(item)
                else:
                    raise TypeError("Queued item is not valid dictionary.")
        logger.info("Terminating Engine Thread")
        self.stop_engine()


    def run_buffer(self, partition_key):
        last_col = self.buffer_record_limit - 1
        last_row = self.number_of_processors - 1
        cur_row = 0
        cur_col = 0
        batch_tracker = {'start_time': time(), 'leftos_collected': False}

        while self.run_engine:
            try:
                item = self.partition_qs[partition_key].get(timeout=self.buffer_time_limit_s)
                # branch 2 - stop engine
                if item == "stop_buffer_worker":
                    break
                # branch 1 - engine running, good data
                elif type(item) is dict:
                    # branch 1.1 - not last row
                    if cur_row < last_row:
                        # branch 1.1a - not last column, continue row
                        if cur_col < last_col:
                            logger.info("Buffering- row {}".format(cur_row))
                            self.buffers[partition_key][cur_row, cur_col] = item
                            cur_col += 1
                        # branch 1.1b - last column, start new row
                        else:
                            self.buffers[partition_key][cur_row, cur_col] = item
                            self.message_q.put(self.buffers[partition_key][cur_row].copy())
                            logger.info("New batch queued")
                            roll_window = self.buffers[partition_key][cur_row, self.buffer_roll_index:]
                            cur_row += 1
                            for n in roll_window:
                                for i in range(abs(self.buffer_roll)):
                                    self.buffers[partition_key][cur_row, i] = n
                            cur_col -= cur_col + self.buffer_roll
                            # REMOVE
                            batch_tracker['start_time'] = time()
                    # branch 1.2 - last row
                    else:
                        # branch 1.2a - not last column, continue row
                        if cur_col < last_col:
                            self.buffers[partition_key][cur_row, cur_col] = item
                            cur_col += 1
                        # branch 1.2b - last column, start return to first row in new cycle
                        else:
                            self.buffers[partition_key][cur_row, cur_col] = item
                            self.message_q.put(self.buffers[partition_key][cur_row].copy())

                            roll_window = self.buffers[partition_key][cur_row, self.buffer_roll_index:]
                            cur_row -= cur_row
                            for n in roll_window:
                                for i in range(abs(self.buffer_roll)):
                                    self.buffers[partition_key][cur_row, i] = n
                            cur_col -= cur_col + self.buffer_roll
                            batch_tracker['start_time'] = time()
                    batch_tracker['leftos_collected'] = False
                # branch 3 bad data
                else:
                    raise TypeError("Queued item is not valid dictionary.")
            except:
            # buffer time max reached, engine still running
                logger.info("Buffer batch timeout exceeded")
                if self.run_engine is True:
                    # engine running, batch timeout with new buffer data (partial row)
                    if cur_col > abs(self.buffer_roll) and batch_tracker['leftos_collected'] is False:
                        logger.info(
                            "Collecting leftovers- pushing partial batch to queue after batch timeout")
                        self.message_q.put(self.buffers[partition_key][cur_row, :cur_col].copy())
                        if cur_row < last_row:
                            cur_row += 1
                        else:
                            cur_row -= cur_row

                        cur_col -= cur_col
                        batch_tracker['start_time'] = time()
                        batch_tracker['leftos_collected'] = True
                    # leftovers already collected
                    else:
                        logger.info("No new data- resetting batch timer")
                        batch_tracker['start_time'] = time()

        logger.info("Terminating Engine Thread")
        self.stop_engine()

    def stop_engine(self):
        self.pipe_conn.close()
        if self.run_engine is True:
            self.run_engine = False
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
        print("done")