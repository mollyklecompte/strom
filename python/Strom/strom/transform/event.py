"""Base event class"""
__version__  = "0.1"
__author__ = "David <david@tura.io>"

class Event(dict):
    def __init__(self,  *args, **kwargs):
        self.update(*args, **kwargs)
        expected_keys = ["event_name", "event_rules", "timestamp", "stream_token", "event_context"]
        bad_keys = []
        for key in self.keys():
            if not key in expected_keys:
                bad_keys.append(key)
        for key in bad_keys:
            del self[key]

        if not "event_name" in self.keys():
            self["event_name"] = ""
        if not "event_rules" in self.keys():
            self["event_rules"] = {}
        if not "timestamp" in self.keys():
            self["timestamp"] = 0
        if not "stream_token" in self.keys():
            self["stream_token"] = ""
        if not "event_context" in self.keys():
            self["event_context"] = {}
