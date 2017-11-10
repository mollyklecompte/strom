"""Base class for dstream"""
__version__  = "0.1"
__author__ = "David <david@tura.io>"


class DStream(dict):
    def __init__(self):
        self["device_id"] = None
        self["version"] = 0
        self["stream_token"] = None
        self["timestamp"] = None
        self["measures"] = {}
        self["fields"] = {}
        self["user_ids"] = {}
        self["tags"] = []
        self["foreign_keys"] = []
        self["filters"] = []
        self["dparam_rules"] = []
        self["event_rules"] = {}


    def _add_measure(self, measure_name, dtype):
        """Creates entry in measures dict for new measure"""
        self["measures"][measure_name] = {"val":None, "dtype":dtype}

    def _add_field(self, field_name):
        self["fields"][field_name] = {}

    def _add_user_id(self, id_name):
        self["user_ids"][id_name] = {}

    def _add_tag(self, tag):
        self["tags"].append(tag)

    def _add_fk(self, foreign_key):
        self["foreign_keys"].appen(foreign_key)

    def _add_filter(self, filter_dict):
        """Add filter to our storage.
         filter_dict: dict of parameters for filter class object"""
        self["filters"].append(filter_dict)

    def _add_derived_param(self, dparam_dict):
        """Add dparam_dict to our dparam_rules
        dparam_dict: dict of the parameters needed to create the derived parameter"""
        self["dparam_reles"].append(dparam_dict)

    def _add_event(self, event_name, event_dict):
        """Add rules for event definition to our storage"""
        self["event_rules"][event_name] = event_dict