"""Class for applying filters to single parameters"""
import numpy as np
from abc import ABCMeta, abstractmethod
from scipy.signal import butter, filtfilt
from .transform import Transformer


class Filter(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    def load_params(self, params):
        """Load func_params dict into filter.params"""
        self.params["func_params"] = params["func_params"]

    def get_params(self):
        """Method to return function default parameters"""
        return self.params["func_params"]

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

class butter_lowpass(Filter):
    """Class to apply a Butterworth lowpass filter to data"""
    def __init__(self):
        super().__init__()
        self.params["func_params"] ={"order":3, "nyquist":0.05}

    def transform_data(self):
        b, a = butter(self.params["func_params"]["order"], self.params["func_params"]["nyquist"])
        buttered_data = {}
        for key in self.data.keys():
            buttered_data[key] = filtfilt(b, a, np.array(self.data[key]["val"]))

        return buttered_data
