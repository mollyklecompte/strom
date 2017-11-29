"""
An StopWatch class.
"""

from time import (time, sleep)
from logging import (INFO, DEBUG, NOTSET)

from strom.utils.logger.logger import logger

__author__ = "Parham <parham@tura.io>"


SECONDS = 1
MILLI_SECONDS = 1000
MICRO_SECONDS = 1000000


class StopWatch:
    """
    StopWatch class.

    Usage:

    """

    def __init__(self):
        self.timers = {}
        self.log = False
        self.log_level = NOTSET
        self.start_time = time()

    def start(self, timer):
        self.timers[timer] = time()
        logger.log(self.log_level, "timer %s set." % timer) if self.log else 0

    def stop(self, timer):
        value = self.clock(timer)
        logger.log(self.log_level, "timer %s stop." % timer) if self.log else 0
        return value

    def clock(self, timer):
        if timer not in self.timers:
            self.timers[timer] = self.start_time
        value = (time() - self.timers[timer]) * MILLI_SECONDS
        logger.log(self.log_level, "timer %s: %0.3f ms" % (timer, value)) if self.log else 0
        return value

    def lap(self, timer):
        pass

    @staticmethod
    def sleep(seconds):
        sleep(seconds)


stopwatch = StopWatch()


__all__ = ["stopwatch", "StopWatch"]
