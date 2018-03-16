"""
Engine Module

It is an engine, it runs. Puts piped data in buffer, spawns processes for the aggregation, transformation and storage of data.

Contains...
- class EngineThread:
manages buffer, moves data from buffer to processing queue
- class Processor:
loads json from data messages to python, runs data transformation + storage process
"""

from multiprocessing import Process, JoinableQueue
from queue import Queue
from threading import Thread
from time import time
from datetime import datetime
import numpy as np
from .processor import Processor
from .data_puller import DataPuller
from strom.dstream.dstream import DStream
from strom.utils.logger.logger import logger

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Engine(Process):
    """
    Based off `threading.Thread`
    Instantiated in server

    Contains buffer, queue for processors, processors, ConsumerThread.
    """

    def __init__(self, engine_conn, processors=4, buffer_roll=0, buffer_max_batch=50, buffer_max_seconds=1, test_mode=False, test_outfile='engine_test_output/engine_test_output'):
        """
        Initializes with empty buffer & queue,
         set # of processors...
        :param processors: number of processors to start
        :type processors: int
        """
        logger.info("Initializing EngineThread")
        super().__init__()
        self.test_run = test_mode
        self.test_outfile = test_outfile
        self.test_batches = {}
        self.pipe_conn = engine_conn
        self.buffers_out_q = JoinableQueue()
        self.number_of_processors = processors
        self.processors = []
        self.run_engine = False
        self.buffer_record_limit = int(buffer_max_batch)
        self.buffer_time_limit_s = float(buffer_max_seconds)
        self.buffers = {}
        self.buffer_in_qs = {}
        self.buffer_workers = {}
        self.data_pullers = {}
        self.buffer_roll = -buffer_roll
        if buffer_roll > 0:
            self.buffer_roll_index = -buffer_roll
        else:
            self.buffer_roll_index = None

    def _init_processors(self):
        """Initializes + starts set number of processors"""
        for n in range(self.number_of_processors):
            processor = Processor(self.buffers_out_q, self.test_run)
            processor.start()
            self.processors.append(processor)

    def _new_buffer(self, partition_key):
        if partition_key not in self.buffers:
            self.buffers[partition_key] = np.array(
                [{0: 0}] * (self.buffer_record_limit * self.number_of_processors)).reshape(
                self.number_of_processors, self.buffer_record_limit)
            if self.test_run:
                self.test_batches[partition_key] = 1
            if partition_key not in self.buffer_in_qs:
                self.buffer_in_qs[partition_key] = Queue()
            else:
                logger.warn(f"New buffer, existing buffer_in_q for stream {partition_key}")
            if partition_key not in self.buffer_workers:
                self.buffer_workers[partition_key] = Thread(target=self.run_buffer,
                                                            args=[partition_key])
                self.buffer_workers[partition_key].start()
            else:
                logger.warn(f"New buffer, existing buffer_worker for stream {partition_key}")
            return True
        else:
            return False

    def _new_data_puller(self, partition_key, template):
        if partition_key not in self.data_pullers:
            if partition_key in self.buffer_in_qs:
                self.data_pullers[partition_key] = DataPuller(template, self.buffer_in_qs[partition_key])
                self.data_pullers[partition_key].start()
            else:
                raise ValueError(f"Attempting to init data_puller for stream {partition_key}, buffer input queue for stream does not exist")
            return True
        else:
            return False

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
                    for q in self.buffer_in_qs.keys():
                        self.buffer_in_qs[q].put("stop_buffer_worker")
                    self.run_engine = False
                    break
                # branch 1 - engine running, good data
                elif type(item) is tuple:
                    partition_key = item[0]['stream_token']
                    new_buffer = self._new_buffer(partition_key)
                    if new_buffer:
                        logger.info(f"Initialized buffer for stream {partition_key}")
                    if item[1] == "new":
                        if item[0]["data_rules"]["pull"] is True:
                            new_puller = self._new_data_puller(partition_key, item[0])
                            if new_puller:
                                print(f"Initialized data puller for stream {partition_key}")
                            else:
                                logger.warn(
                                    f"Attempting to initialize data puller for stream {partition_key} - puller already exists")
                    elif item[1] == "load":
                        self.buffer_in_qs[partition_key].put(item[0])
                    else:
                        raise TypeError(
                            "Invalid tuple in pipe - index 1 must be str 'load' or str 'new'")
                else:
                    raise TypeError("Invalid item in pipe")
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
                item = self.buffer_in_qs[partition_key].get(timeout=self.buffer_time_limit_s)
                # branch 2 - stop engine
                if item == "stop_buffer_worker":
                    break
                # branch 1 - engine running, good data
                elif isinstance(item, DStream) or (type(item) is dict and "stream_token" in item.keys()):
                    if item["data_rules"]["date_format"] is not None:
                        item["timestamp"] = datetime.strptime(item["timestamp"], item["data_rules"]["date_format"])
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
                            if self.test_run:
                                self.buffers_out_q.put((self.buffers[partition_key][cur_row].copy(), f"{self.test_outfile}_{partition_key}_{self.test_batches[partition_key]}.txt"))
                                self.test_batches[partition_key] += 1
                            else:
                                self.buffers_out_q.put(self.buffers[partition_key][cur_row].copy())
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
                            if self.test_run:
                                self.buffers_out_q.put((self.buffers[partition_key][cur_row].copy(), f"{self.test_outfile}_{partition_key}_{self.test_batches[partition_key]}.txt"))
                                self.test_batches[partition_key] += 1
                            else:
                                self.buffers_out_q.put(self.buffers[partition_key][cur_row].copy())

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
                        if self.test_run:
                            self.buffers_out_q.put((self.buffers[partition_key][cur_row, :cur_col].copy(), f"{self.test_outfile}_{partition_key}_{self.test_batches[partition_key]}.txt"))
                            self.test_batches[partition_key] += 1
                        else:
                            self.buffers_out_q.put(self.buffers[partition_key][cur_row, :cur_col].copy())
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

    def stop_engine(self):
        self.pipe_conn.close()
        if self.run_engine is True:
            self.run_engine = False
        for p in self.data_pullers.keys():
            self.data_pullers[p].pulling = False
        logger.info(self.buffers_out_q.qsize())
        self.buffers_out_q.join()
        logger.info("Queue joined")
        for p in self.processors:
            logger.info("Putting poison pills in Q")
            self.buffers_out_q.put("666_kIlL_thE_pROCess_666")
        logger.info("Poison pills done")
        for p in self.processors:
            p.join()
            logger.info("Engine shutdown- processor joined")
        print("done")