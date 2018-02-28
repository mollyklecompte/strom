import json
import os
from multiprocessing import Process
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

    def _write_test_data(self, data, outfile):

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

                if self.test_run:
                    data = queued[0].tolist()
                    self._write_test_data(f"{json.dumps(data)}\n", outfile=queued[1])
                else:
                    data = queued.tolist()
                    coordinator.process_data(data, data[0]["stream_token"])

            self.q.task_done()