"""
B-stream class

Initializes a Bstream dict off Dstream, using a Dstream template to initialize all keys, static values. The Bstream contains methods to aggregate measures, timestamps, user ids, fields and tags, as well as a wrapper aggregate method.
"""
import pandas as pd

from strom.transform.apply_transformer import apply_transformation
from strom.utils.logger.logger import logger
from .dstream import DStream

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class BStream(DStream):
    def __init__(self, template, dstreams):
        logger.debug("init BStream")
        super().__init__()
        self.dstreams = dstreams
        self["template_id"] = template["_id"]
        self._load_from_dict(template)
        self["stream_token"] = str(template["stream_token"])

    def _load_from_dict(self, dictionary):
        for key in dictionary.keys():
            if key != "_id":
                self[key] = dictionary[key]
                logger.debug("added key %s" % key)

    def _aggregate_measures(self):
        logger.debug("aggregating measures")
        all_measures = [s["measures"] for s in self.dstreams]
        self["measures"] = {
            m: {
                'val': [i[m]['val'] for i in all_measures],
                'dtype': v['dtype']
            } for m, v in self["measures"].items()
        }

    def _aggregate_uids(self):
        logger.debug("aggregating uids")
        uids = [s["user_ids"] for s in self.dstreams]
        self["user_ids"] = {
            uidkey: [i[uidkey] for i in uids] for uidkey, v in self["user_ids"].items()
        }

    def _aggregate_ts(self):
        logger.debug("aggregating timestamps")
        self["timestamp"] = [s["timestamp"] for s in self.dstreams]

    def _aggregate_fields(self):
        logger.debug("aggregating fields")
        fields = [s["fields"] for s in self.dstreams]
        self["fields"] = {
            fieldkey: [i[fieldkey] for i in fields] for fieldkey, v in self["fields"].items()
        }

    def _aggregate_tags(self):
        logger.debug("aggregating tags")
        tags = [s["tags"] for s in self.dstreams]
        self["tags"] = {
            tagkey: [i[tagkey] for i in tags] for tagkey, v in self["tags"].items()
        }
    def _measure_df(self):
        logger.debug("aggregating into DataFrame")
        all_measures = [s["measures"] for s in self.dstreams]
        self["new_measures"] = {
            m: [i[m]['val'] for i in all_measures] for m, v in self["measures"].items()
        }
        self["new_measures"]["timestamp"] = self["timestamp"]
        self["new_measures"] = pd.DataFrame(self["new_measures"])

    def prune_dstreams(self):
        logger.debug("removing input dstreams to save space")
        self.dstreams = None

    @property
    def aggregate(self):
        logger.debug("aggregating everything")
        self._aggregate_uids()
        self._aggregate_measures()
        self._aggregate_ts()
        self._aggregate_fields()
        self._aggregate_tags()
        self._measure_df()

        return self

    def partition_data(self, parition_key, partition_value):
        pass

    def apply_filters(self):
        logger.debug("applying filters")
        self["filter_measures"] = {}
        for filter_rule in self["filters"]:
            logger.debug("applying filter %s" % (filter_rule["filter_name"]))
            self["filter_measures"][filter_rule["filter_name"]] = apply_transformation(filter_rule, self)[filter_rule["filter_name"]]

    def apply_dparam_rules(self):
        logger.debug("deriving parameters")
        self["derived_measures"] = {}
        for dparam_rule in self["dparam_rules"]:
            logger.debug("deriving %s" % (dparam_rule["measure_rules"]["output_name"]))
            self["derived_measures"][dparam_rule["measure_rules"]["output_name"]] = apply_transformation(dparam_rule, self)[dparam_rule["measure_rules"]["output_name"]]

    def find_events(self):
        logger.debug("finding events")
        self["events"] = {}
        for event_rule in self["event_rules"].values():
            logger.debug("finding event %s" % (event_rule["event_name"]))
            self["events"][event_rule["event_name"]] = apply_transformation(event_rule, self)
