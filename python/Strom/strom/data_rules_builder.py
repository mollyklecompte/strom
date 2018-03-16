from strom.data_map_builder import build_mapping


def build_data_rules(source_mapping_list, dstream_mapping_list, puller=None):
    map_list = build_mapping(source_mapping_list, dstream_mapping_list)
    data_rules = {"mapping_list": map_list}
    if not puller:
        data_rules["pull"] = False
        data_rules["puller"] = {}
    elif type(puller) is list:
        data_rules["pull"] = True
        data_rules["puller"]["type"] = puller[0]
        data_rules["puller"]["inputs"] = {i[0]: i[1] for i in puller[1]}

    return data_rules
