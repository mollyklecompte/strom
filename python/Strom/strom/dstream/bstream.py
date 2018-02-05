"""
B-stream class

Initializes a Bstream dict off Dstream, using a Dstream template to initialize all keys, static values. The Bstream contains methods to aggregate measures, timestamps, user ids, fields and tags, as well as a wrapper aggregate method.
"""
import json

from strom.transform.dataframe_event import *
from strom.transform.derive_dataframe import *
from strom.transform.filter_dataframe import *
from .dstream import DStream

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"

def dummy_function(input, dummy_param):
    """I am making this to test things, it just returns the input with not_names"""
    for column in input:
        input = input.rename( columns={column:"not_"+column})

    return input


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
        self["fields"] = [json.dumps(s["fields"]) for s in self.dstreams]


    def _aggregate_tags(self):
        logger.debug("aggregating tags")
        self["tags"] = [json.dumps(s["tags"]) for s in self.dstreams]

    def _measure_df(self):
        logger.debug("aggregating into DataFrame")
        all_measures = [s["measures"] for s in self.dstreams]
        self["measures"] = {
            m: [i[m]['val'] for i in all_measures] for m, v in self["measures"].items()
        }
        self["measures"]["timestamp"] = self["timestamp"]
        for user_id, value in self["user_ids"].items():
            self["measures"][user_id] = value

        self["measures"]["tags"] = self["tags"]
        self["measures"]["fields"] = self["fields"]

        self["measures"] = pd.DataFrame.from_dict(self["measures"])

    def prune_dstreams(self):
        logger.debug("removing input dstreams to save space")
        self.dstreams = None

    @property
    def aggregate(self):
        logger.debug("aggregating everything")
        self._aggregate_uids()
        self._aggregate_ts()
        self._aggregate_fields()
        self._aggregate_tags()
        self._measure_df()

        return self

    def partition_rows(self, parition_key, partition_value, comparison_operator="=="):
        """This function takes a column name (partition_key), the value to compare with (parition_value)
        and the operator for comparision (comparison_operator)
        It returns the boolean index for the rows that meet the patition condition"""
        logger.debug("Finding the row indices that meet the partition condition")
        comparisons= {"==":np.equal, "!=":np.not_equal, ">=":np.greater_equal, "<=":np.less_equal, ">":np.greater, "<":np.less}
        cur_comp = comparisons[comparison_operator]
        return cur_comp(self["measures"][parition_key], partition_value)

    def partition_data(self, list_of_partitions, logical_comparison="AND"):
        """This function takes a list of tuples of partition parameters used by partition_rows() and
        returns all rows from the measure DataFrame that meet the logical AND or logical OR of those conditions"""
        logger.debug("building parition rows")
        if logical_comparison == "AND":
            start_bools = np.ones((self["measures"].shape[0],),dtype=bool)
        elif logical_comparison == "OR":
            start_bools = np.zeros((self["measures"].shape[0],), dtype=bool)
        else:
            raise ValueError("{} is not a supported logical comparision".format(logical_comparison))

        for partition in list_of_partitions:
            new_inds = self.partition_rows(partition[0], partition[1], partition[2])
            if logical_comparison == "AND":
                start_bools = np.logical_and(start_bools, new_inds)
            elif logical_comparison == "OR":
                start_bools = np.logical_or(start_bools, new_inds)
            else:
                raise ValueError("{} is not a supported logical comparision".format(logical_comparison))

        return self["measures"][start_bools]

    @staticmethod
    def select_transform(transform_type, transform_name):
        """Method to grab the transform function from the correct module"""
        logger.debug("Selecting transform function")
        available_transforms = {}
        available_transforms["filter_data"] = {
                                               "ButterLowpass":ButterLowpass,
                                               "WindowAverage":WindowAverage,
                                               "dummy":dummy_function
                                               }
        available_transforms["derive_param"] = {
                                                "DeriveSlope":DeriveSlope,
                                                "DeriveChange":DeriveChange,
                                                "DeriveCumsum":DeriveCumsum,
                                                "DeriveDistance":DeriveDistance,
                                                "DeriveHeading":DeriveHeading,
                                                "DeriveWindowSum":DeriveWindowSum,
                                                "DeriveScaled":DeriveScaled,
                                                "DeriveInBox":DeriveInBox,
                                                }
        available_transforms["detect_event"] = {"DetectThreshold":DetectThreshold}
        return available_transforms[transform_type][transform_name]

    def add_columns(self, new_data_frame):
        self["measures"] = self["measures"].join(new_data_frame, rsuffix="_r", lsuffix="_l")

    def apply_transform(self, partition_list, measure_list, transform_type, transform_name, param_dict, logical_comparison="AND"):
        """This function takes uses the inputs to partition the measures DataFrame, apply the specified
        transform to the specified columns with the supplied parameters and joins the results to the
        measures DataFrame"""
        logger.debug("Applying transform")
        selected_data = self.partition_data(partition_list, logical_comparison)[measure_list] #partition rows then select columns
        tranformer = self.select_transform(transform_type,transform_name) #grab your transformer
        transformed_data = tranformer(selected_data, param_dict) #Return data, either as array or DataFrame
        #Concatonate data with self["measures"]
        if transform_type != "detect_event":
            self.add_columns(transformed_data)

        return transformed_data


    def apply_filters(self):
        logger.debug("applying filters")
        self["filter_measures"] = {}
        for filter_rule in self["filters"]:
            # logger.debug("applying filter {}".format(filter_rule["param_dict"]["filter_name"]))
            self.apply_transform(filter_rule["partition_list"], filter_rule["measure_list"], filter_rule["transform_type"], filter_rule["transform_name"], filter_rule["param_dict"], filter_rule["logical_comparision"])


    def apply_dparam_rules(self):
        logger.debug("deriving parameters")
        self["derived_measures"] = {}
        for dparam_rule in self["dparam_rules"]:
            print("deriving {}".format(dparam_rule["param_dict"]["measure_rules"]["output_name"]))
            self.apply_transform(dparam_rule["partition_list"], dparam_rule["measure_list"], dparam_rule["transform_type"], dparam_rule["transform_name"], dparam_rule["param_dict"], dparam_rule["logical_comparison"])


    def find_events(self):
        logger.debug("finding events")
        self["events"] = {}
        for event_name, event_rule in self["event_rules"].items():
            print("finding event {}".format(event_name))
            self["events"][event_name] = self.apply_transform(event_rule["partition_list"], event_rule["measure_list"], event_rule["transform_type"], event_rule["transform_name"], event_rule["param_dict"], event_rule["logical_comparison"])
