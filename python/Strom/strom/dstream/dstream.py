"""Base class for dstream"""
import uuid

__version__  = "0.1"
__author__ = "David <david@tura.io>"


class DStream(dict):
    def __init__(self):
        self["device_id"] = None
        self["version"] = 0
        self["stream_token"] = uuid.uuid1()
        self["storage_rules"] = {}
        self["ingest_rules"] = {}
        self["engine_rules"] = {}
        self["timestamp"] = None
        self["measures"] = {}
        self["fields"] = {}
        self["user_ids"] = {}
        self["tags"] = []
        self["foreign_keys"] = {}
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
        self["foreign_keys"][foreign_key] = {}

    def _add_filter(self, filter_dict):
        """Add filter to our storage.
         filter_dict: dict of parameters for filter class object"""
        self["filters"].append(filter_dict)

    def _add_derived_param(self, dparam_dict):
        """Add dparam_dict to our dparam_rules
        dparam_dict: dict of the parameters needed to create the derived parameter"""
        self["dparam_rules"].append(dparam_dict)

    def _add_event(self, event_name, event_dict):
        """Add rules for event definition to our storage"""
        self["event_rules"][event_name] = event_dict

    def _publish_version(self):
        """Increment version number"""
        self["version"] += 1

    def define_dstream(self, storage_rules, ingestion_rules, measure_list, field_names, user_id_names,
                      tag_list, filter_list, dparam_rule_list, event_list):
        """Inputs:
        storage_rules: dict containing storage rules
        ingestion_rules: dict containing ingestion rules
        measure_list: list of tuples: (measure_name, dtype) for each measure supplied to the stream
        field_names: list of field names
        user_id_names: list of user_id names
        tag_list: list of tags
        filter_list: list of filter rules which are dicts
        dparam_rule_list: list of dparam_rules, which are dicts
        event_list: list of tuples: (event_name, event_rules)
        """

        self["storage_rules"] = storage_rules
        self["ingest_rules"] = ingestion_rules
        for measure_name, dtype in measure_list:
            self._add_measure(measure_name, dtype)

        for field in field_names:
            self._add_field(field)

        for uid in user_id_names:
            self._add_user_id(uid)

        self["tags"].extend(tag_list)
        self["filters"].extend(filter_list)
        self["dparam_rules"].extend(dparam_rule_list)

        for event_name, event_rules in event_list:
            self._add_event(event_name, event_rules)

        self["version"] += 1
