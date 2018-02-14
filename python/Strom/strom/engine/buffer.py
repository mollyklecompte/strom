"""
Buffer Module

Buffer class stores dstreams.
"""

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Buffer(list):
    def __init__(self, rolling_window=0):
        super().__init__()
        if rolling_window > 0:
            self.rolling_window = -rolling_window
        else:
            self.rolling_window = None

    def reset(self):
        del self[:self.rolling_window]