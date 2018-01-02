"""Class for applying filters to single measures"""
from abc import ABCMeta, abstractmethod

import numpy as np
from scipy.signal import butter, filtfilt
from strom.utils.logger.logger import logger

from .transform import Transformer


def window_data(in_array, window_len):
    """
    :param in_array: input array
    :type in_array: numpy array
    :param window_len: length of window for averaging
    :type window_len: int
    :return: numpy array of windowed average of the data
    """
    logger.debug("Windowing data with window length %d" % (window_len))
    w_data = np.convolve(in_array, np.ones(window_len), "valid") / window_len
    # Dealing with the special case for endpoints of in_array
    ends_divisor = np.arange(1, window_len, 2)
    start = np.cumsum(in_array[:window_len - 1])[::2] / ends_divisor
    stop = (np.cumsum(in_array[:-window_len:-1])[::2] / ends_divisor)[::-1]
    if in_array.shape[0] - w_data.shape[0] - start.shape[0] < stop.shape[0]:
        stop = stop[1:]
    return np.concatenate((start, w_data, stop))

class Filter(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    def load_params(self, params):
        """Load func_params dict into filter.params
        :param params: all the parameters needed for the filter
        "type params: dict containing keys "func_params" and "filter_name"
        """
        logger.debug("loading func_params and filter_name")
        self.params["func_params"] = params["func_params"]
        self.params["filter_name"] = params["filter_name"]

    def get_params(self):
        """Method to return function default parameters
        :return: dict of parameters
        """
        return self.params["func_params"]

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

class ButterLowpass(Filter):
    """Class to apply a Butterworth lowpass filter to data"""
    def __init__(self):
        super().__init__()
        self.params["func_params"] ={"order":3, "nyquist":0.05}
        self.params["filter_name"] =  "buttered"
        logger.debug("initialized ButterLowpass. Use get_params() to see parameter values")

    @staticmethod
    def butter_data(data_array, order, nyquist):
        """
        fuction for applying a butter lowpass filtuer to data
        :param data_array: array of data to be filtered
        :type data_array: numpy array
        :param order: order of the filter
        :type order: int
        :param nyquist: Wn parameter from scipy.signal.butter
        :type nyquist: float
        :return:
        """
        logger.debug("buttering data")
        b, a = butter(order, nyquist)
        return filtfilt(b, a, data_array)

    def transform_data(self):
        """
        apply butterworth lowpass filter and return the transformed data
        :return: dict of {"filter_name: numpy array of filtered data}
        """
        buttered_data = {}
        logger.debug("transforming_data")
        for key in self.data.keys():
            key_array = np.array(self.data[key]["val"], dtype=float)
            if len(key_array.shape) > 1:
                buttered_data[self.params["filter_name"]] = np.zeros(key_array.shape)
                for ind in range(key_array.shape[1]):
                    buttered_data[self.params["filter_name"]][:,ind] = self.butter_data(key_array[:,ind],
                                                                                        self.params["func_params"]["order"],
                                                                                        self.params["func_params"]["nyquist"])
            else:
                buttered_data[self.params["filter_name"]] = self.butter_data(key_array,
                                                                             self.params["func_params"]["order"],
                                                                             self.params["func_params"]["nyquist"])
        return buttered_data

class WindowAverage(Filter):
    """Class for windowed average to smooth data"""
    def __init__(self):
        super().__init__()
        self.params["func_params"] = {"window_len": 2}
        self.params["filter_name"] = "windowed"
        logger.debug("initialized WindowAverage. Use get_params() to see parameter values")


    def transform_data(self):
        """
        Apply windowed average to data and return the results
        :return: dict of {"filter_name: numpy array of filtered data}
        """
        logger.debug("transforming_data")
        windowed_data = {}
        for key in self.data.keys():
            key_array = np.array(self.data[key]["val"], dtype=float)
            if len(key_array.shape) > 1:
                windowed_data[self.params["filter_name"]] = np.zeros(key_array.shape)
                for ind in range(key_array.shape[1]):
                    windowed_data[self.params["filter_name"]][:,ind] = window_data(key_array[:,ind],
                                                  self.params["func_params"]["window_len"])
            else:
                windowed_data[self.params["filter_name"]] = window_data(key_array,
                                                                        self.params["func_params"]["window_len"])

        return windowed_data
