"""Base class for DStream
This dictionary defines our basic data structure.
A user registers a new data source with our server module which then uses this class to create a
template for that data source and the template is stored by our Coordinator class. All data sent
to our server for that data source will be in the format defined by this DStream object. The
BStream subclass then aggregates the individual DStream objects.
"""
import uuid

from strom.utils.logger.logger import logger

__version__  = "0.1"
__author__ = "David <david@tura.io>"


class DStream(dict):
    def __init__(self):
        """
        Initializes and empty DStream with a unique stream token. All the other expected keys are
        initialized as empty data structures of the desired type.
        """
        self["stream_name"] = None
        self["user_description"] = None
        self["version"] = 0
        self["stream_token"] = uuid.uuid1()
        self["source_key"] = None
        self["template_id"] = uuid.uuid1()
        self["storage_rules"] = {}
        self["ingest_rules"] = {}
        self["engine_rules"] = {}
        self["timestamp"] = None
        self["measures"] = {}
        self["fields"] = {}
        self["user_ids"] = {}
        self["tags"] = {}
        self["foreign_keys"] = []
        self["filters"] = []
        self["dparam_rules"] = []
        self["event_rules"] = {}
        self["data_rules"] = {}
        logger.debug("DStream initialize")

    def add_measure(self, measure_name, dtype):
        """Creates entry in measures dict for new measure"""
        self["measures"][measure_name] = {"val":None, "dtype":dtype}
        logger.debug("added measure %s" % (measure_name))

    def add_field(self, field_name):
        self["fields"][field_name] = {}
        logger.debug("added field %s" % (field_name))

    def add_user_id(self, id_name):
        self["user_ids"][id_name] = {}
        logger.debug("added id_name %s" % (id_name))

    def add_tag(self, tag_name):
        self["tags"][tag_name] = {}
        logger.debug("added tag %s" % (tag_name))

    def add_fk(self, foreign_key):
        self["foreign_keys"].append({foreign_key: None})
        logger.debug("added key %s" % (foreign_key))

    def add_filter(self, filter_dict):
        """Add filter to our storage.
         filter_dict: dict of parameters for filter class object"""
        filter_dict["filter_id"] = uuid.uuid1()
        self["filters"].append(filter_dict)
        logger.debug("added filter")

    def add_derived_param(self, dparam_dict):
        """Add dparam_dict to our dparam_rules
        dparam_dict: dict of the parameters needed to create the derived parameter"""
        dparam_dict["dparam_id"] = uuid.uuid1()
        self["dparam_rules"].append(dparam_dict)
        logger.debug("added derived parameter")

    def add_event(self, event_name, event_dict):
        """Add rules for event definition to our storage"""
        event_dict["event_id"] = uuid.uuid1()
        self["event_rules"][event_name] = event_dict
        logger.debug("adding event %s" % (event_name))

    def publish_version(self):
        """Increment version number"""
        self["version"] += 1
        logger.debug("version now %s" % (str(self['version'])))

    def add_measures(self, measure_list):
        for measure_name, dtype in measure_list:
            self.add_measure(measure_name, dtype)

    def add_filters(self, filter_list):
        for filter in filter_list:
            self.add_filter(filter)

    def add_dparams(self, dparam_list):
        for dparam in dparam_list:
            self.add_derived_param(dparam)

    def add_events(self, event_list):
        for event_name, event_rules in event_list: # list of (name, rules dict) tuples
            self.add_event(event_name, event_rules)

    def add_user_ids(self, ids_list):
        for uid in ids_list:
            self.add_user_id(uid)

    def add_fields(self, field_names):
        for field in field_names:
            self.add_field(field)

    def add_tags(self, tag_list):
        for tag_name in tag_list:
            self.add_tag(tag_name)

    def add_foreign_keys(self, fks):
        for fk in fks:
            self.add_fk(fk)

    def add_data_rules(self, rules_dict):
        self["data_rules"] = rules_dict

    def load_from_json(self, json_file):
        """
        The standard method for loading data from an existing json dict.
        :param json_file: the json dict containing data to be loaded into our DStream
        :type json_file: dict
        """
        for key in json_file.keys():
            if key != 'stream_token' and key != 'template_id':
                self[key] = json_file[key]
                logger.debug("added key %s" % (key))


class DataDStream(DStream):
    def __init__(self):
        logger.debug("initializing DataDStream")
        super().__init__()