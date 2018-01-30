"""Class for creating derived parameters from measures
This is a subclass of the Transformer class that creates derived measures from input measures.
The process of deriving measures takes input measures and uses them to calculate a new measure
such as finding distance from a positional measure or the slope of an input measure. Derived
measures may be of difference dimension than their inputs.
These DeriveParam functions are called by apply_transformer on BStream data and the results are
stored as BStream["derived_measures"]
"""

import numpy as np

from strom.utils.logger.logger import logger
from .filter_dataframe import window_data


def sloper(rise_array, run_array, window_len):
    """
    Function to calculate slope of two rates of change using the classic rise over run formula
    :param rise_array: data for numerator of rise/run
    :type rise_array: numpy array
    :param run_array: data for denominator of rise/run
    :type run_array: numpy array
    :param window_len: length of window for averaging slope
    :type window_len: int
    :return: rise_array/run_array
    :rtype: numpy array
    """
    sloped = rise_array / run_array
    if window_len > 1:
        sloped = window_data(sloped, window_len)

    return sloped


def DeriveSlope(data_frame, params=None):
    logger.debug("Starting DeriveSlope.")

    if params==None:
        params = {}
        params["func_params"] = {"window":1}
        params["measure_rules"] ={
                                    "rise_measure":"measure y values (or rise in rise/run calculation of slope)",
                                    "run_measure":"measure containing x values (or run in rise/run calculation of slope)",
                                    "output_name":"name of returned measure"
                                    }
        return params

    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    window_len = self.params["func_params"]["window"]
    xrun = np.array(self.data[self.params["measure_rules"]["run_measure"]]["val"], dtype=float)
    yrise = np.array(self.data[self.params["measure_rules"]["rise_measure"]]["val"], dtype=float)
    smaller_len = np.min([xrun.shape[0], yrise.shape[0]])
    sloped = self.sloper(yrise[:smaller_len,], xrun[:smaller_len,], window_len)
    return {self.params["measure_rules"]["output_name"]:sloped}


def diff_data(data_array, window_len, angle_diff):
    """
    Function to calculate the difference between samples in an array
    :param data_array: input data
    :type data_array: numpy array
    :param window_len: window length for smoothing
    :type window_len: int
    :param angle_diff: Specify if the angular difference between samples should be calculated instead of raw diff
    :type angle_diff: Boolean
    :return: diff between samples in data_array
    :rtype: numpy array
    """
    logger.debug("diffing data")
    diffed_data = np.diff(data_array)
    if angle_diff:
        diffed_data = (diffed_data + 180.0) % 360 - 180
    if window_len > 1:
        diffed_data = window_data(diffed_data, window_len)
    return diffed_data

def DeriveChange(data_frame, params=None):
    logger.debug("initialized DeriveChange. Use get_params() to see parameter values")
    if params==None:
        params = {}
        params["func_params"] = {"window":1, "angle_change":False}
        params["measure_rules"] ={
                                    "target_measure":"measure_name",
                                    "output_name":"name of returned measure"
                                    }
        return params

    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    window_len = self.params["func_params"]["window"]
    target_array = np.array(self.data[self.params["measure_rules"]["target_measure"]]["val"], dtype=float)
    diffed_data = self.diff_data(target_array, window_len, self.params["func_params"]["angle_change"])
    return {self.params["measure_rules"]["output_name"]:diffed_data}


def cumsum(data_array, offset=0):
    """
    Calculate the cumulative sum of a vector
    :param data_array: data to be summed
    :type data_array: numpy array
    :param offset: starting value for sum
    :type offset: float
    :return: the cumulative sum of the data_array
    :rtype: numpy array
    """
    logger.debug("cumsum")
    return np.cumsum(data_array)+offset


def DeriveCumsum(data_frame, params=None):
    logger.debug("initialized DeriveCumsum. Use get_params() to see parameter values")
    if params == None:
        params = {}
        params["func_params"] = {"offset":0}
        params["measure_rules"] = {
                                    "target_measure":"measure_name",
                                    "output_name":"name of returned measure"
                                    }
        return params

    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    target_array = np.array(self.data[self.params["measure_rules"]["target_measure"]]["val"], dtype=float)
    cumsum_array = self.cumsum(target_array, self.params["func_params"]["offset"])
    return {self.params["measure_rules"]["output_name"]:cumsum_array}


def euclidean_dist(position_array, window_len):
    """
    Function to calculate euclidean distance between consecutive samples in a positional vector
    :param position_array: input vector of positions
    :type position_array: N x 2 numpy array
    :param window_len: length of window for averaging output
    :type window_len: int
    :return: distances between consecutive points
    :rtype: (N-1) x 1 numpy array
    """
    logger.debug("calculating euclidean distance")
    euclid_array = np.sqrt(np.sum(np.diff(position_array, axis=0)**2, axis=1))
    if window_len > 1:
        euclid_array = window_data(euclid_array, window_len)
    return  euclid_array

def great_circle(position_array, window_len, units="mi"):
    """
    Function to calculate the great circle distance between consecutive samples in lat lon vector
    :param position_array: input vector of lat lon points
    :type position_array: N x 2 numpy array
    :param window_len: length of window for averaging output
    :type window_len: int
    :param units: String for output units. Currently 'mi' and 'km' supported
    :type units: str
    :return: distances between consecutive points
    :rtype: (N-1) x 1 numpy array

    """
    logger.debug("calculating great circle distance")
    lat1 = position_array[:-1, 0]
    lat2 = position_array[1:, 0]
    lon1 = position_array[:-1, 1]
    lon2 = position_array[1:, 1]
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlat = lat1 - lat2
    dlon = lon1 - lon2
    inner_val = np.sin(dlat/2.0)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2.0)**2
    outer_val = 2*np.arcsin(np.sqrt(inner_val))
    if units == "mi":
        earth_diameter = 3959
    elif units == "km":
        earth_diameter = 6371

    great_dist = outer_val*earth_diameter
    if window_len > 1:
        great_dist = window_data(great_dist, window_len)
    return great_dist


def DeriveDistance(data_frame, params=None):
    logger.debug("initialized DeriveDistance. Use get_params() to see parameter values")
    if params == None:
        params = {}
        params["func_params"] = {
                                    "window":1,
                                    "distance_func": "euclidean",
                                    "swap_lon_lat":False
                                 }
        params["supported_distances"] = ["euclidean", "great_circle"]
        params["measure_rules"] = {
                                    "spatial_measure":"name of geo-spatial measure",
                                    "output_name":"name of returned measure"
                                    }

    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    window_len = self.params["func_params"]["window"]
    position_array = np.array(self.data[self.params["measure_rules"]["spatial_measure"]]["val"], dtype=float)
    if self.params["func_params"]["swap_lon_lat"]:
        position_array = position_array[:,[1, 0]]
    if self.params["func_params"]["distance_func"] == "euclidean":
        dist_array = self.euclidean_dist(position_array, window_len)
    elif self.params["func_params"]["distance_func"] == "great_circle":
        dist_array = self.great_circle(position_array, window_len)

    return {self.params["measure_rules"]["output_name"]:dist_array}


def flat_angle(position_array, window_len, units="deg"):
    """
    Function for computing the angle between samples from a 2D plane
    :param position_array:input vector of positions
    :type position_array: N x 2 numpy array
    :param window_len: length of window for smoothing
    :type window_len: int
    :param units: deg or rad for degrees or radians
    :type units: str
    :return: the angle between consecutive samples
    :rtype: (N-1) x 1 numpy array
    """
    logger.debug("finding cartesian angle of vector")
    diff_array = np.diff(position_array, axis=0)
    diff_angle = np.arctan2(diff_array[:,1], diff_array[:,0])
    if units == "deg":
        diff_angle = (np.rad2deg(diff_angle) + 360 ) % 360
    elif units == "rad":
        diff_angle = (diff_angle + 2*np.pi) % np.pi
    if window_len > 1:
        diff_angle = window_data(diff_angle, window_len)
    return diff_angle

def bearing(position_array, window_len, units="deg"):
    """
    Calculates the angle between lat lon points
    :param position_array: input vector of lat lon points
    :type position_array: N x 2 numpy array
    :param window_len: Length of window for averaging
    :type window_len: int
    :param units: String for output units. Currently 'mi' and 'km' supported
    :type units: str
    :return: the angle between consecutive latlon points
    :rtype: (N - 1) x 1 numpy array
    """
    logger.debug("finding bearing of vector")
    lat1 = position_array[:-1, 0]
    lat2 = position_array[1:, 0]
    lon1 = position_array[:-1, 1]
    lon2 = position_array[1:, 1]
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon1 - lon2
    first_val = np.sin(dlon)*np.cos(lat2)
    second_val = np.cos(lat1)*np.sin(lat2)-np.sin(lat1)*np.cos(lat2)*np.cos(dlon)
    cur_bear = np.arctan2(first_val, second_val)
    if units == "deg":
        cur_bear = (np.rad2deg(cur_bear) + 360 ) % 360
    if window_len > 1:
        cur_bear = window_data(cur_bear, window_len)
    return cur_bear


def DeriveHeading(data_frame, params=None):
    logger.debug("initialized DeriveHeading. Use get_params() to see parameter values")
    if params == None:
        params = {}
        params["func_params"] = {"window":1, "units":"deg", "heading_type":"bearing", "swap_lon_lat":False}
        params["measure_rules"] = {
                                    "spatial_measure":"name of geo-spatial measure",
                                    "output_name":"name of returned measure"
                                    }

    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    window_len = self.params["func_params"]["window"]
    position_array = np.array(self.data[self.params["measure_rules"]["spatial_measure"]]["val"], dtype=float)
    if self.params["func_params"]["swap_lon_lat"]:
        position_array = position_array[:,[1, 0]]
    if self.params["func_params"]["heading_type"] == "bearing":
        angle_array = self.bearing(position_array, window_len, self.params["func_params"]["units"])
    elif self.params["func_params"]["heading_type"] == "flat_angle":
        angle_array = self.flat_angle(position_array, window_len, self.params["func_params"]["units"])

    return {self.params["measure_rules"]["output_name"]:angle_array}


def window_sum(in_array, window_len):
    """
    Calculates and returns windowed sum of input vector
    :param in_array: input array to be summed
    :type in_array: numpy array
    :param window_len: length of window around each point to be summed
    :type window_len: int
    :return: vector of sums
    :rtype: numpy array
    """
    logger.debug("Summing the data with window length %d" % (window_len))
    w_data = np.convolve(in_array, np.ones(window_len), "valid")
    # Dealing with the special case for endpoints of in_array
    start = np.cumsum(in_array[:window_len - 1])
    start = start[int(np.floor(window_len/2.0))::]
    stop = np.cumsum(in_array[:-window_len:-1])
    stop = stop[int(np.floor(window_len/2.0))-1::][::-1]
    if in_array.shape[0] - w_data.shape[0] - start.shape[0] < stop.shape[0]:
        logger.debug("Window size did not divide easily into input vector length. Adjusting the endpoint values")
        stop = stop[:-1]
    return np.concatenate((start, w_data, stop))


def DeriveWindowSum(data_frame, params=None):
    logger.debug("Starting DeriveWindowSum.")

    if params == None:
        params = {}
        params["func_params"] = {"window":2}
        params["measure_rules"] =  {"target_measure":"measure_name", "output_name":"name of returned measure"}

    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    window_len = self.params["func_params"]["window"]
    target_array = np.array(self.data[self.params["measure_rules"]["target_measure"]]["val"], dtype=float)
    summed_data = self.window_sum(target_array, window_len)
    return  {self.params["measure_rules"]["output_name"]:summed_data}


def scale_data(in_array, scalar):
    """
    Multiply an array by a scaler
    :type in_array: numpy array
    :type scalar: float
    :rtype: numpy array
    """
    return scalar*in_array

def DeriveScaled(data_frame, params=None):
    logger.debug("Starting DeriveScaled.")

    if params == None:
        params = {}
        params["func_params"] = {"scalar":1}
        params["measure_rules"] =  {"target_measure":"measure_name", "output_name":"name of returned measure"}

    logger.debug("transforming data to %s" %(self.params["measure_rules"]["output_name"]))
    target_array = np.array(self.data[self.params["measure_rules"]["target_measure"]]["val"], dtype=float)
    scaled_out =self.scale_data(target_array, self.params["func_params"]["scalar"])
    return  {self.params["measure_rules"]["output_name"]:scaled_out}

def in_box(spatial_array, upper_left, lower_right):
    """
    Given a positional vector and the corners of a box, find which of the point are in the box
    :param spatial_array: spatial input data
    :type spatial_array: N x 2 numpy array
    :param upper_left: coordinates of upper left corner of box
    :type upper_left: 2 x 1 numpy array
    :param lower_right: coordinates of lower right corner of box
    :type lower_right: 2 x 1 numpyh array
    :return: True if a sample point is in the box or False if it is not
    :rtype: N x 1 Boolean numpy array
    """
    return np.logical_and(np.logical_and(spatial_array[:, 0] >= upper_left[0],
                                         spatial_array[:, 0] <= lower_right[0]),
                          np.logical_and(spatial_array[:, 1] <= upper_left[1],
                                         spatial_array[:, 1] >= lower_right[1]))


def DeriveInBox(data_frame, params=None):
    logger.debug("Starting DeriveInBox.")

    if params == None:
        params = {}
        params["func_params"] = {"upper_left_corner":(0,1), "lower_right_corner":(1,0)}
        params["measure_rules"] = {"spatial_measure":"name of geo-spatial measure", "output_name":"name of returned measure"}
    logger.debug("transforming data to %s" % (self.params["measure_rules"]["output_name"]))
    spatial_array = np.array(self.data[self.params["measure_rules"]["spatial_measure"]]["val"], dtype=float)
    box_bool = self.in_box(spatial_array, self.params["func_params"]["upper_left_corner"], self.params["func_params"]["lower_right_corner"])
    return {self.params["measure_rules"]["output_name"]:box_bool}



