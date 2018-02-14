"""Class for applying filters to measures
This is a subclass of the Transformer class that creates filtered measures from input measures.
Filters seek to smooth, clean and remove artifacts from the data and all filters  return data of
the same dimensions as their input.
These filters are called by apply_transformer on BStream data and the results are stored
as BStream["filtered_measures"]

window_data is used as a filter but also has uses in other Transformer subclasses so it is a top
level function for easier importing by those subclasses"""

import numpy as np
import pandas as pd
from scipy.signal import butter, filtfilt

from strom.utils.logger.logger import logger


def window_data(in_array, window_len):
    """
    Function that calculates the windowed average of a vector
    :param in_array: input array
    :type in_array: numpy array
    :param window_len: length of window for averaging
    :type window_len: int
    :return: windowed average of the data
    :rtype: numpy array
    """
    logger.debug("Windowing data with window length {:d}".format(window_len))
    w_data = np.convolve(in_array, np.ones(window_len), "valid") / window_len
    # Dealing with the special case for endpoints of in_array
    ends_divisor = np.arange(1, window_len, 2)
    start = np.cumsum(in_array[:window_len - 1])[::2] / ends_divisor
    stop = (np.cumsum(in_array[:-window_len:-1])[::2] / ends_divisor)[::-1]
    if in_array.shape[0] - w_data.shape[0] - start.shape[0] < stop.shape[0]:
        stop = stop[1:]
    return np.concatenate((start, w_data, stop))


def butter_data(data_array, order, nyquist):
    """
    Fuction for applying a butter lowpass filter to data
    :param data_array: array of data to be filtered
    :type data_array: numpy array
    :param order: order of the filter
    :type order: int
    :param nyquist: Wn parameter from scipy.signal.butter
    :type nyquist: float
    :return: filtered data
    :rtype: numpy array
    """
    logger.debug("buttering data")
    b, a = butter(order, nyquist)
    return filtfilt(b, a, data_array)



def ButterLowpass(data_frame, params=None):
    """Class to apply a Butterworth lowpass filter to data"""
    logger.debug("Calculating ButterLowpass.")
    if params==None:
        params ={"order":3, "nyquist":0.05, "filter_name":"_buttered"}
        return params
    elif "filter_name" not in params:
        params["filter_name"] = "_buttered"

    logger.debug("transforming_data")
    buttered_data = np.zeros(data_frame.shape)
    output_names = []
    for col_ind, df_col in enumerate(data_frame):
        buttered_data[:, col_ind] = butter_data(data_frame[df_col].values, params["order"], params["nyquist"])
        output_names.append(df_col+params["filter_name"])

    return pd.DataFrame(data=buttered_data, columns=output_names, index=data_frame.index)


def WindowAverage(data_frame, params=None):
    """Class for using a windowed average to smooth data"""
    logger.debug("Calculating WindowAverage.")
    if params == None:
        params = {"window_len": 2, "filter_name":"_windowed"}
        return params
    elif "filter_name" not in params:
        params["filter_name"] = "_windowed"

    logger.debug("transforming_data")
    windowed_data = np.zeros(data_frame.shape)
    output_names = []
    for col_ind, df_col in enumerate(data_frame):
        windowed_data[:, col_ind] = window_data(data_frame[df_col].values, params["window_len"])
        output_names.append(df_col + params["filter_name"])

    return pd.DataFrame(data=windowed_data, columns=output_names, index=data_frame.index)
