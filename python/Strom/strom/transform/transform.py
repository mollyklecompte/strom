"""Abstract class for our transform functions.
 These transforms are called by apply_transforms module in order to create filtered measures,
 derived measures and detect events. Data is loaded into the transform object
 from a BStream as well a dict with the parameters for the given transformation.
 The Transformer then uses the parameters to apply the transformation to the data
 """
from abc import ABCMeta, abstractmethod

from strom.utils.logger.logger import logger


class Transformer(object):
    """How this fits in and is used by apply transforms """
    __metaclass__ = ABCMeta
    def __init__(self):
        logger.debug("initializing transformer")
        self.data = {}
        self.params = {}

    def add_measure(self, measure_name, measure_data):
        """Method to select the parameter for transformation
        :param measure_name: name of measure
        :type measure_name: str
        :param measure_data: the data for measure_name
        :type measure_data: dict
        """
        self.data[measure_name] = measure_data
        logger.debug("added measure %s" % (measure_name))

    def load_measures(self, measure_dict):
        """Load a dict of measures into transformation
        :param measure_dict: dictionary containing multiple measures following BStream formatting
        :type measure_dict: dict
        """
        logger.debug("added measure dict")
        for key, value in measure_dict.items():
            self.add_measure(key, value)

    @abstractmethod
    def load_params(self, params):
        """Method for setting the parameters of the transformation
        :param params: all the parameters needed for the transformation
        :type params: dict
        """
        raise NotImplementedError("subclass must implement this abstract method.")

    @abstractmethod
    def get_params(self):
        """Method to return transformer's default parameters"""
        raise NotImplementedError("subclass must implement this abstract method.")


    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

