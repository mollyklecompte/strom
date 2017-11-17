"""
B-stream class.
"""

from Strom.strom.dstream.dstream import DStream

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class BStream(DStream):
    def __init__(self, template, dstreams):
        super().__init__()
        self.dstreams = dstreams
        self.template_id = template["_id"]
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


