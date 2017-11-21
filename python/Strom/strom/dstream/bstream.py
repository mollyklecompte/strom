"""
B-stream class

Initializes a Bstream dict off Dstream, using a Dstream template to initialize all keys, static values. The Bstream contains methods to aggregate measures, timestamps, user ids, fields and tags, as well as a wrapper aggregate method.
"""

from Strom.strom.dstream.dstream import DStream
from Strom.strom.transform.apply_transformer import apply_transformation

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class BStream(DStream):
    def __init__(self, template, dstreams, ids):
        super().__init__()
        self.dstreams = dstreams
        self.ids = ids
        self["template_id"] = template["_id"]
        self._load_from_dict(template)

    # def _set_from_temp(self):
        # self["stream_name"] = self.template["stream_name"]
        # self["version"] = self.template["version"]
        # self["stream_token"] = self.template["stream_token"]
        # self["sources"] = self.template["sources"]
        # self["storage_rules"] = self.template["storage_rules"]
        # self["ingest_rules"] = self.template["ingest_rules"]
        # self["engine_rules"] = self.template["engine_rules"]
        # self["fields"] = self.template["fields"]
        # self["user_ids"] = self.template["user_ids"]
        # self["tags"] = self.template["tags"]
        # self["foreign_keys"] = self.template["foreign_keys"]
        # self["filters"] = self.template["filters"]
        # self["dparam_rules"] = self.template["dparam_rules"]
        # self["event_rules"] = self.template["event_rules"]

    def _load_from_dict(self, dict):
        for key in dict.keys():
            if key != "_id":
                self[key] = dict[key]

    def _aggregate_measures(self):
        all_measures = [s["measures"] for s in self.dstreams]
        self["measures"] = {m: {'val': [i[m]['val'] for i in all_measures], 'dtype': v['dtype']} for m, v in self["measures"].items()}

    def _aggregate_uids(self):
        uids = [s["user_ids"] for s in self.dstreams]
        self["user_ids"] = {uidkey: [i[uidkey] for i in uids] for uidkey, v in self["user_ids"].items()}

    def _aggregate_ts(self):
        self["timestamp"] = [s["timestamp"] for s in self.dstreams]

    def _aggregate_fields(self):
        fields = [s["fields"] for s in self.dstreams]
        self["fields"] = {fieldkey: [i[fieldkey] for i in fields] for fieldkey, v in self["fields"].items()}

    def _aggregate_tags(self):
        tags = [s["tags"] for s in self.dstreams]
        self["tags"] = {tagkey: [i[tagkey] for i in tags] for tagkey, v in self["tags"].items()}

    def aggregate(self):
        self._aggregate_uids()
        self._aggregate_measures()
        self._aggregate_ts()
        self._aggregate_fields()
        self._aggregate_tags()

        return self

    def apply_filters(self):
        self["filter_measures"] = {}
        for filter_rule in self["filters"]:
            self["filter_measures"][filter_rule["filter_name"]] = apply_transformation(filter_rule, self)[filter_rule["filter_name"]]


    def apply_dparam_rules(self):
        self["derived_measures"] = {}
        for dparam_rule in self["dparam_rules"]:
            self["derived_measures"][dparam_rule["measure_rules"]["output_name"]] = apply_transformation(dparam_rule, self)[dparam_rule["measure_rules"]["output_name"]]

    def find_events(self):
        self["events"] = {}
        for event_rule in self["event_rules"]:
            self["events"][event_rule["event_name"]] = apply_transformation(event_rule, self)
