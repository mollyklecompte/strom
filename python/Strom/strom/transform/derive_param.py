"""Class for creating derived parameters from measures"""
import numpy as np
from abc import ABCMeta, abstractmethod
from .transform import Transformer
from .filter_data import window_data


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
                                       "run_measure":"measure containing x values (or run in rise/run calculation of slope)",
                                       "output_name":"name of returned measure"}

    def transform_data(self):
        window_len = self.params["func_params"]["window"]
        xrun = np.array(self.data[self.params["measure_rules"]["run_measure"]]["val"])
        yrise = np.array(self.data[self.params["measure_rules"]["rise_measure"]]["val"])
        dx = np.diff(xrun)
        dy = np.diff(yrise)


        if window_len > 1:
            sloped =  window_data(dy/dx, window_len)
        else:
            sloped = dy/dx
        return {self.params["measure_rules"]["output_name"]:sloped}

class DeriveChange(DeriveParam):
    def __init__(self):
        super().__init__()
        self.params["func_params"] = {"window":1}
        self.params["measure_rules"] ={"target_measure":"measure_name", "output_name":"name of returned measure"}

    def transform_data(self):
        window_len = self.params["func_params"]["window"]
        diffed_data = np.diff(np.array(self.data[self.params["measure_rules"]["target_measure"]]["val"]))
        if window_len > 1:
            diffed_data = window_data(diffed_data, window_len)

        return {self.params["measure_rules"]["output_name"]:diffed_data}

class DeriveDistance(DeriveParam):
    def __init__(self):
        super().__init__()
        self.params["func_params"] = {"window":1, "distance_func": "euclidean"}
        self.params["supported_distances"] = ["euclidean", "great_circle"]
        self.param["measure_rules"] = {"spatial_measure":"name of geo-spatial measure", "output_name":"name of returned measure"}
    @staticmethod
    def euclidean_dist(position_array):
        return np.sum(np.diff(position_array, axis=0)**2, axis=1)
    @staticmethod
    def great_circle(position_array, units="mi"):
        diff_array = np.diff(position_array, axis=0)
        dlat = diff_array[:,0]
        dlon = diff_array[:,1]
        inner_val = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
        outer_val = 2*np.arcsin(np.sqrt(inner_val))
        if units == "mi":
            earth_diameter = 3959
        elif units = "km":
            earth_diameter = 6371

        return outer_val*earth_diameter


    def transform_data(self):
        window_len = self.params["func_params"]["window"]
        position_array = np.array(self.data[self.params["measure_rules"]["saptial_measure"]["val"]])
        if self.params["func_params"]["distance_func"}] == "euclidean":
            dist_array = self.euclidean_dist(position_array)
        elif self.params["func_params"]["distance_func"}] == "great_circle":
            dist_array = self.great_circle(position_array)

        if window_len > 1:
            dist_array = window_data(dist_array, window_len)

        return {self.params["measure_rules"]["output_name"]:dist_array}
