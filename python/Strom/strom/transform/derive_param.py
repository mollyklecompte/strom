"""Class for creating derived parameters from measures"""
import numpy as np
from abc import ABCMeta, abstractmethod
from .transform import Transformer


def window_data(in_array, window_len):
    w_data = np.convolve(in_array, np.ones(window_len), "valid") / window_len
    r = np.arange(1, window_len - 1, 2)  # Dealing with the special case for endpoints of in_array
    start = np.cumsum(in_array[:window_len - 1])[::2] / r
    stop = (np.cumsum(in_array[:-window_len:-1])[::2] / r)[::-1]
    return np.concatenate((start, w_data, stop))


class DeriveParam(Transformer):
    __metaclass__ = ABCMeta

    def __init__(self):
        super().__init__()

    def load_params(self, params):
        self.params["func_params"] = params["func_params"]
        self.params["measure_rules"] = params["measure_rules"]

    def get_params(self):
        """Method to return function default parameters"""
        return {"func_params":self.params["func_params"], "measure_rules":self.params["measure_rules"]}

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")


class DeriveSlope(DeriveParam):
    def __init__(self):
        super().__init__()
        self.params["func_params"] = {"window":1}
        self.params["measure_rules"] ={"rise_measure":"measure y values (or rise in rise/run calculation of slope)",
                                       "run_measure":"measure containing x values (or run in rise/run calculation of slope)"}

    def transform_data(self):
        window_len = self.params["func_params"]["window"]
        yrise = np.array(self.data[self.params["measure_rules"]["rise_measure"]]["val"])
        xrun = np.array(self.data[self.params["measure_rules"]["run_measure"]]["val"])
        dx = np.diff(xrun)
        dy = np.diff(yrise)


        if window_len > 1:
            dx = window_data(dx, window_len)
            dy = window_data(dy, window_len)
        return dy/dx