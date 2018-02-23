"""
Engine Module

It is an engine, it runs. Puts piped data in buffer, spawns processes for the aggregation, transformation and storage of data.

Contains...
- class EngineThread:
manages buffer, moves data from buffer to processing queue
- class Processor:
loads json from data messages to python, runs data transformation + storage process
"""

import numpy as np
from multiprocessing import Process, JoinableQueue
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
            if type(queued) is str:
                if queued == "666_kIlL_thE_pROCess_666":
                    # self.is_running = False
                    break
            else:
                data = queued.tolist()
                coordinator.process_data(data, data[0]["stream_token"])
                # data_list = [datum for datum in queued]
                # for data in data_list:
                #     coordinator.process_data_async(data, data[0]["stream_token"])
            self.q.task_done()

class EngineThread(Process):
    """
    Based off `threading.Thread`
    Instantiated in server

    Contains buffer, queue for processors, processors, ConsumerThread.
    """

    def __init__(self, engine_conn, processors=4, buffer_roll=0, buffer_max_batch=50, buffer_max_seconds=1):
        """
        Initializes with empty buffer & queue,
         set # of processors...
        :param processors: number of processors to start
        :type processors: int
        """
        logger.info("Initializing EngineThread")
        super().__init__()
        self.pipe_conn = engine_conn
        self.message_q = JoinableQueue()
        self.number_of_processors = processors
        self.processors = []
        self.run_engine = False
        self.buffer_record_limit = int(buffer_max_batch)
        self.buffer_time_limit_s = float(buffer_max_seconds)
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


    def run(self):
        """
        Sets up numpy array buffer and puts stuff in and gets stuff out
        """
        self._init_processors()
        self.run_engine = True

        last_col = self.buffer_record_limit - 1
        last_row = self.number_of_processors - 1
        cur_row = 0
        cur_col = 0
        batch_tracker = {'start_time': time(), 'leftos_collected': False}

        def put_in_buffer(datum):
            self.buffer[cur_row, cur_col] = datum

        while self.run_engine:
            while time() - batch_tracker['start_time'] < self.buffer_time_limit_s:
                if self.pipe_conn.poll():
                    item = self.pipe_conn.recv()
                    # branch 2 - stop engine
                    if item == "stop_poison_pill":
                        self.run_engine = False
                        break
                    # branch 1 - engine running, good data
                    elif type(item) is dict:
                        # branch 1.1 - not last row
                        if cur_row < last_row:
                            # branch 1.1a - not last column, continue row
                            if cur_col < last_col:
                                logger.info("Buffering- row {}".format(cur_row))
                                put_in_buffer(item)
                                cur_col += 1
                            # branch 1.1b - last column, start new row
                            else:
                                put_in_buffer(item)
                                self.message_q.put(self.buffer[cur_row].copy())
                                logger.info("New batch queued")
                                roll_window = self.buffer[cur_row, self.buffer_roll_index:]
                                cur_row += 1
                                for n in roll_window:
                                    for i in range(abs(self.buffer_roll)):
                                        self.buffer[cur_row, i] = n
                                cur_col -= cur_col + self.buffer_roll
                                batch_tracker['start_time'] = time()
                        # branch 1.2 - last row
                        else:
                            # branch 1.2a - not last column, continue row
                            if cur_col < last_col:
                                put_in_buffer(item)
                                cur_col += 1
                            # branch 1.2b - last column, start return to first row in new cycle
                            else:
                                put_in_buffer(item)
                                self.message_q.put(self.buffer[cur_row].copy())
                                roll_window = self.buffer[cur_row, self.buffer_roll_index:]
                                cur_row -= cur_row
                                for n in roll_window:
                                    for i in range(abs(self.buffer_roll)):
                                        self.buffer[cur_row, i] = n
                                cur_col -= cur_col + self.buffer_roll
                                batch_tracker['start_time'] = time()
                        batch_tracker['leftos_collected'] = False
                    # branch 3 bad data
                    else:
                        raise TypeError("Queued item is not valid dictionary.")
            # buffer time max reached, engine still running
            logger.info("Buffer batch timeout exceeded")
            if self.run_engine is True:
                # engine running, batch timeout with new buffer data (partial row)
                if cur_col >= abs(self.buffer_roll) and batch_tracker['leftos_collected'] is False:
                    logger.info("Collecting leftovers- pushing partial batch to queue after batch timeout")
                    self.message_q.put(self.buffer[cur_row, :cur_col].copy())
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
        print("done")