"""
An StopWatch class.

This module has public member called stopwatch which is a collection of Timers.

You can use it as following:

    stopwatch['timer_1'].start()
    ...
    stopwatch['timer_1'].lap()
    ...
    stopwatch['timer_1'].time()
    ...
    stopwatch.sleep(0.5)
    ...
    stopwatch['timer_1'].stop()

"""

from time import (time, sleep)
from logging import (NOTSET, INFO, DEBUG, WARNING, CRITICAL, WARN, ERROR, FATAL)
from enum import (Enum, auto)

from strom.utils.logger.logger import logger

__author__ = "Parham <parham@tura.io>"


SECONDS = 1
MILLI_SECONDS = 1000
MICRO_SECONDS = 1000000


class TimerStatus(Enum):
    STOPPED = auto()
    PAUSED = auto()
    RUNNING = auto()


class Timer:
    """
    Timer class.
    """

    def __init__(self, name='DEFAULT', output_to_logger=True, logger_level=INFO):
        self.name = name
        self.laps = []
        self._start_time = time()
        self._running_time = 0
        self.status = TimerStatus.STOPPED
        self.output_to_logger = output_to_logger
        self.logger_level = logger_level

    def __setattr__(self, key, value):
        if key == 'status' and not isinstance(value, TimerStatus):
                raise ValueError
        elif key == 'logger_level' and value not in [NOTSET, DEBUG, INFO, WARNING, CRITICAL, WARN, FATAL, ERROR]:
            raise ValueError
        elif key == 'output_to_logger' and not isinstance(value, bool):
            raise ValueError
        # all good set the attribute
        super().__setattr__(key, value)

    def start(self):
        self._start_time = time()
        self.status = TimerStatus.RUNNING
        logger.log(self.logger_level, "timer %s started." % self.name) if self.output_to_logger else 0

    def pause(self):
        self._running_time = self.time()
        self._start_time = time()
        self.status = TimerStatus.PAUSED
        logger.log(self.logger_level, "timer %s paused. time: %0.3f ms" % (self.name, self._running_time)) if self.output_to_logger else 0
        return self._running_time

    def reset(self):
        self.status = TimerStatus.STOPPED
        logger.log(self.logger_level, "timer %s reset." % self.name) if self.output_to_logger else 0
        pass

    def stop(self):
        t = self.time()
        self._start_time = time()
        self._running_time = 0
        self.status = TimerStatus.STOPPED
        logger.log(self.logger_level, "timer %s stopped. time: %0.3f ms" % (self.name, t)) if self.output_to_logger else 0
        return t

    def time(self):
        t = self._running_time + (time() - self._start_time) * MILLI_SECONDS
        logger.log(self.logger_level, "timer %s time: %0.3f ms" % (self.name, t)) if self.output_to_logger else 0
        return t

    def read(self):
        return self.time()

    def lap(self):
        t = self.stop()
        self.laps.append(t)
        logger.log(self.logger_level, "timer %s lapped. lap time: %0.3f ms" % (self.name, t)) if self.output_to_logger else 0
        return t

    def print(self):
        print("timer %s: %0.3f ms" % (self.name, self.time()))

    def log(self, level=INFO):
        logger.log(level, "timer %s: %0.3f ms" % (self.name, self.time()))


class StopWatch(dict):
    """
    StopWatch class.

    Usage:

    """

    def __init__(self):
        self.__timers = {}

    def __setitem__(self, key, value):
        raise AttributeError

    def __getitem__(self, item):
        if isinstance(item, str):
            if item in self.__timers:
                return self.__timers[item]
            else:
                timer = Timer(name=item)
                self.__timers[item] = timer
                return timer
        else:
            ValueError

    @staticmethod
    def sleep(seconds):
        sleep(seconds)


# global stopwatch member
stopwatch = StopWatch()

# import all default module classes and members
__all__ = ["stopwatch", "Timer", "StopWatch"]
