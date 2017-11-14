"""Class for applying filters to single parameters"""
import numpy as np
from abc import ABCMeta, abstractmethod
from scipy.signal import butter, filtfilt
from transform import Transformer


class Filter(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

class butter_lowpass(Filter):
    def __init__(self):
        super().__init__()
        self.params["func_name"] = "butter_lowpass"
        self.params["func_params"] ={"order":3, "nyquist":0.05}

    def transform_data(self):
        b, a = butter(self.params["func_params"]["order"], self.params["func_params"]["nyquist"])
        buttered_data = {}
        for key in self["measures"].keys():
            buttered_data[key] = filtfilt(b, a, list(self["measures"].values())[0])

        return buttered_data
