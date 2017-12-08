"""Base event class"""
__version__  = "0.1"
__author__ = "David <david@tura.io>"

from strom.utils.logger.logger import logger

class Event(dict):
    def __init__(self,  *args, **kwargs):
        self.update(*args, **kwargs)
        logger.debug("initializing event")
        expected_keys = ["event_name", "event_rules", "timestamp", "stream_token", "event_context"]
        bad_keys = []
        for key in self.keys():
            if not key in expected_keys:
                bad_keys.append(key)
                logger.debug("non-expected key found: %s" % (key))
        for key in bad_keys:
            del self[key]

        if not "event_name" in self.keys():
            logger.warning("No event_name found")
            self["event_name"] = ""
        if not "event_rules" in self.keys():
            logger.warning("No event_rules found")
            self["event_rules"] = {}
        if not "timestamp" in self.keys():
            logger.debug("no timestamp supplied")
            self["timestamp"] = 0
        if not "stream_token" in self.keys():
            logger.warning("no stream_token supllied")
            self["stream_token"] = ""
        if not "event_context" in self.keys():
            logger.debug("No context")
            self["event_context"] = {}
