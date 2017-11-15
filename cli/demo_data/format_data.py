import json
single_data = json.load(open("single_data.txt"))
ds_template = json.load(open("demo_template.txt"))

print(ds_template)

out_json = ds_template.copy()
out_json["timestamp"] = single_data["timestamp"]
out_json["measures"]["location"]["val"] = single_data["location"]
out_json["fields"]["region-code"] = single_data["region-code"]
out_json["user_ids"]["id"] = single_data["id"]
out_json["user_ids"]["driver-id"] = single_data["driver-id"]

json.dump(out_json, open("demo_single_data.txt", 'w'))

data_log = json.load(open("data_log.txt"))

out_list = []
for in_json in data_log:
    out_json = ds_template.copy()
    out_json["timestamp"] = single_data["timestamp"]
    out_json["measures"]["location"]["val"] = single_data["location"]
    out_json["fields"]["region-code"] = single_data["region-code"]
    out_json["user_ids"]["id"] = single_data["id"]
    out_json["user_ids"]["driver-id"] = single_data["driver-id"]
    out_list.append(out_json)

json.dump(out_list,open("demo_data_log.txt", "w"))

