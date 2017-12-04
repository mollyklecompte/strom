import json, copy
from strom.dstream.bstream import BStream
### Usage run 'python -m Strom.demo_data.format_data' in strom/python directory of repo ###

demo_data_dir = "demo_data/"
single_data = json.load(open(demo_data_dir+"single_data.txt"))
ds_template = json.load(open(demo_data_dir+"demo_template.txt"))

print(ds_template)

out_json = ds_template.copy()
out_json["timestamp"] = single_data["timestamp"]
out_json["measures"]["location"]["val"] = single_data["location"]
out_json["fields"]["region-code"] = single_data["region-code"]
out_json["user_ids"]["id"] = single_data["id"]
out_json["user_ids"]["driver-id"] = single_data["driver-id"]

json.dump(out_json, open(demo_data_dir+"demo_single_data.txt", 'w'))

data_log = json.load(open(demo_data_dir+"data_log.txt"))

out_list = []
out_single_trip = []
for in_json in data_log:
    out_json = ds_template.copy()
    out_json["timestamp"] = in_json["timestamp"]
    out_json["measures"]["location"]["val"] = in_json["location"]
    out_json["fields"]["region-code"] = in_json["region-code"]
    out_json["user_ids"]["id"] = in_json["id"]
    out_json["user_ids"]["driver-id"] = in_json["driver-id"]
    out_list.append(copy.deepcopy(out_json))
    if in_json["id"] == 26:
        out_single_trip.append(copy.deepcopy(out_json))
print(len(out_list))

json.dump(out_list, open(demo_data_dir+"demo_data_log.txt", "w"))
json.dump(out_single_trip, open(demo_data_dir+"demo_trip26.txt", "w"))

ds_template["_id"] = "crowley"
tmp_b = BStream(ds_template, out_single_trip)
tmp_b.aggregate
tmp_b.apply_filters()
tmp_b.apply_dparam_rules()
tmp_b.find_events()
json.dump(tmp_b, open(demo_data_dir+"demo_bstream_trip26.txt", "w"))