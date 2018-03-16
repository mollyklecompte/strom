from copy import deepcopy

from strom.data_map_builder import build_mapping
from strom.dstream.dstream import DStream
from strom.fun_update_guide import update_guide
from strom.event_dict_builder import event_builder_rules
from strom.transform_rules_builder import FilterBuilder, DParamBuilder

__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"


filter_builder = FilterBuilder()
dp_builder = DParamBuilder()


def build_rules_from_event(event: str, base_measures: list, **kwargs):
    # event: str, event type (key in event_builder_rules)
    # base_measure: tuple, name + type i.e. ('location', 'geo')
    base_event = event_builder_rules[event]
    # for t in base_event['base_measure_types']:
    #     if t not in [m[1] for m in base_measures]:
    #         raise ValueError(
    #             f"Event {base_event} requires base measure type(s) {base_event['base_measure_types']}")


    missing_inputs = []
    for i in base_event['required_input_settings'] + base_event['required_event_inputs']:
        if i not in kwargs.keys():
            missing_inputs.append(i)
    if len(missing_inputs):
        raise ValueError (f"Missing required inputs {missing_inputs}")
    else:
        inputs = kwargs
        partition_list = inputs.pop('partition_list')
        stream_id = inputs.pop('stream_id')
        fn = base_event['callback']
        rules = fn(*[m[0] for m in base_measures], partition_list, stream_id, **inputs)

        return rules


def build_data_rules(source_mapping_list, dstream_mapping_list, puller=None):
    # if puller, it should be a list of form [type, inputs: list of len2 k,v lists)
    if source_mapping_list is not None and dstream_mapping_list is not None:
        map_list = build_mapping(source_mapping_list, dstream_mapping_list)
    else:
        map_list = []
    data_rules = {"mapping_list": map_list}
    data_rules["puller"] = {}
    if not puller:
        data_rules["pull"] = False
    elif type(puller) is list:
        data_rules["pull"] = True
        data_rules["puller"]["type"] = puller[0]
        data_rules["puller"]["inputs"] = {i[0]: i[1] for i in puller[1]}
    else:
        raise TypeError("Invalid puller- must be list or None")

    return data_rules


def build_measure_list(measure_rules: list):
    return [msr for rule in measure_rules if type(rule) is tuple and len(rule) == 4 for msr in rule[0]]


def build_filter_list(measure_list, filters: list):
    return [filter_builder.build_rules_dict(f[0], f[1]) for f in filters]


def get_filtered_measures(filter_list):
    return [f"{m}{f['param_dict']['filter_name']}" for f in filter_list for m in f['measure_list']]


def add_filters_to_measure_rules(measure_rules: list, filtered_measures: list):
    rules_filters = []
    for rule in measure_rules:
        rule = list(rule)
        rule[1] = [(f, m[1]) for f in filtered_measures for m in rule[0] if m[0] in f]
        rules_filters.append(rule)
    return rules_filters

def create_template_dstream(strm_nm, src_key, measures: list, uids: list, events: list, dparam_rules: list, filters: list, data_rules: dict, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, tags=None, fields=None):
    # measures: list of tuples (measure_name, dtype)

    template = DStream()
    template['stream_name'] = strm_nm
    template['source_key'] = src_key
    template.add_measures(measures)  # expects list of tuples (measure, dtype)
    template.add_user_ids(uids)
    template['user_description'] = usr_dsc
    template.add_dparams(dparam_rules)
    template.add_events(events)
    template.add_filters(filters)
    template.add_data_rules(data_rules)

    if storage_rules is not None:  # add else branch w default
        template['storage_rules'] = storage_rules
    else:
        template['storage_rules'] = {"store_raw":True, "store_filtered":True, "store_derived":True}

    if ingest_rules is not None:  # add else branch w default
        template['ingest_rules'] = ingest_rules

    if engine_rules is not None:  # add else branch w default
        template['engine_rules'] = engine_rules

    # the totally optional section
    if tags is not None:
        template.add_tags(tags)

    if foreign_keys is not None:
        template.add_foreign_keys(foreign_keys)

    if fields is not None:
        template.add_fields(fields)

    return template


def build_events_dparams_from_measure_rules(measure_rules: list):
    event_list = []
    dparam_list = []

    for rule in measure_rules:
        if len(rule[3]):
            for event in rule[3]:
                event_build_measures = []
                for m in event[2]:
                    if m in [msr[0] for msr in rule[0]]:
                        for msr in rule[0]:
                            if m == msr[0]:
                                event_build_measures.append(msr)
                    elif m in [f[0] for f in rule[1]]:
                        for f in rule[1]:
                            if m == f[0]:
                                event_build_measures.append(f)
                    else:
                        raise ValueError(f"Event build missing measure {m}")
                rule_set = build_rules_from_event(event[0], event_build_measures, **event[1])
                event_list.append(rule_set['event_rules'])
                dparam_list.extend(rule_set['dparam_rules'])
        if len(rule[2]):
            for dp in rule[2]:
                dp_build_measures = []
                for measure, val in dp[2].items():
                    if val in [msr[0] for msr in rule[0]]:
                        for msr in rule[0]:
                            if val == msr[0]:
                                dp_build_measures.append((measure, val))
                    elif val in [f[0] for f in rule[1]]:
                        for f in rule[1]:
                            if val == f[0]:
                                dp_build_measures.append((f, val))
                    else:
                        raise ValueError(f"Derived param build missing measure {measure}")

                for bm in dp_build_measures:
                    dp[1][bm[0]] = bm[1]
                dparam_list.append(dp_builder.build_rules_dict(dp[0], dp[1]))

    return dparam_list, event_list


def build_template(strm_nm, src_key, measure_rules: list, uids: list, filters: list, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, tags=None, fields=None, source_mapping_list=None, dstream_mapping_list = None, puller=None):
    # measure_rules is list of tuples of form
    # ([measure name, measure type], [filtered measures], [dparams], [event_tups])
    # filter tuples: ('name', {inputs})
    # dparam tuples: ('name', {inputs}, {'target_measure': 'measure'})
    # event_tups are tuples of form (event name, {kwargs}, [measure1, filtered_measure2])
    # for example:
    # (['location', 'geo'], ['buttery_location'], [('turn', {kwargs})])

    measure_list = build_measure_list(measure_rules)

    # build filters
    filter_list = build_filter_list(measure_list, filters)

    # all filtered measures
    filtered_measures = get_filtered_measures(filter_list)

    # if measure in measure_rules is filtered, filtered measure added to list at index 1
    measure_rules_filters = add_filters_to_measure_rules(measure_rules, filtered_measures)

    rules = build_events_dparams_from_measure_rules(measure_rules_filters)
    dparam_list = rules[0]
    event_list = rules[1]

    data_rules = build_data_rules(source_mapping_list, dstream_mapping_list, puller)

    template = create_template_dstream(strm_nm, src_key, measure_list, uids, event_list, dparam_list, filter_list, data_rules, usr_dsc, storage_rules, ingest_rules, engine_rules, foreign_keys, tags, fields)
    return template


def build_new_rule_updates(template_dstream, filter_tups: list, measure_tups: list):
    if len(measure_tups):
        measure_list = build_measure_list(measure_tups)
    else:
        measure_list = []
    avail_measures = [(measure, measure_dict['dtype']) for measure, measure_dict in template_dstream['measures'].items()]
    if len(measure_list):
        new_measures = [m for m in measure_list if m not in avail_measures]
    else:
        new_measures = []
    avail_measures.extend(new_measures)
    filter_list = build_filter_list(avail_measures, filter_tups)
    filtered_measures = get_filtered_measures(filter_list)
    measure_rules_filters = add_filters_to_measure_rules(measure_tups, filtered_measures)

    rules = build_events_dparams_from_measure_rules(measure_rules_filters)
    new_dparams = rules[0] # list of dicts
    new_events = rules[1] # list of dicts

    new_measures_updates = [{'field': 'measures', 'type': 'new', 'args': [m], 'kwargs': {}} for m in new_measures]
    new_filter_updates = [{'field': 'filters', 'type': 'new', 'args': [f], 'kwargs': {}} for f in filter_list]
    new_dparam_updates = [{'field': 'dparam_rules', 'type': 'new', 'args': [d], 'kwargs': {}} for d in new_dparams]
    new_event_updates = [{'field': 'event_rules', 'type': 'new', 'args': [e[0], e[1]], 'kwargs': {}} for e in new_events]

    updates = []
    for i in [new_measures_updates, new_filter_updates, new_dparam_updates, new_event_updates]:
        updates.extend(i)
    return updates


def update(template, flat_updates: list, new_measure_tups: list, new_filter_tups: list):
    updates = flat_updates
    updates.extend(build_new_rule_updates(template, new_filter_tups, new_measure_tups))
    update_result = update_template(template, updates)
    return update_result



# update wrapper
def update_template(template_json, updates_list: list):
    template = DStream()
    template.load_from_json(template_json)
    template['stream_token'] = template_json['stream_token']
    old_template = deepcopy(template)

    for update in updates_list:
        update_fn = update_guide[update['field']][update['type']]['function']
        args = []
        args.append(template)
        if update_guide[update['field']][update['type']]['field_key_arg']:
            args.append(update['field'])
        args.extend(update['args'])
        kwargs = update['kwargs']
        update_fn(*args, **kwargs)

    if valid_update(template) is True:
        template.publish_version()
        return 'ok', template
    else:
        return 'invalid update', old_template, valid_update(template)


def valid_update(template: DStream):
    bad_updates = []

    # possible names of filtered measures
    possible_filtered_measures = ['{}{}'.format(m, f_name) for m in template['measures'].keys() for f_name in [ f['param_dict']['filter_name'] for f in template['filters']]]

    possible_filtered_measures.extend(
        ['{}{}'.format('timestamp', f_name) for f_name in [ f['param_dict']['filter_name'] for f in template['filters']]]
    )

    # measure required by filter missing
    bad_updates.extend([('filter', f['transform_name'], 'measure', m) for f in template['filters'] for m in f['measure_list'] if m not in template['measures'].keys() and m != 'timestamp'])

    # print('MEASURE NAMES', possible_filtered_measures, template['measures'].keys(), [dp['param_dict']['measure_rules']['output_name'] for dp in template['dparam_rules']])

    # measure required by derived param missing
    bad_updates.extend([('derived param', dp['transform_name'], 'measure', m) for dp in template['dparam_rules'] for m in dp['measure_list'] if m not in template['measures'].keys() and m not in possible_filtered_measures and m not in [dp['param_dict']['measure_rules']['output_name'] for dp in template['dparam_rules']] and m != 'timestamp'])

    bad_updates.extend([('event', event_key, 'derived param', m) for event_key, event_dict in template['event_rules'].items() for m in event_dict["measure_list"] if m not in [dp['param_dict']['measure_rules']['output_name'] for dp in template['dparam_rules']] and m not in possible_filtered_measures and m not in template['measures'].keys() and m != 'timestamp'])

    if len(bad_updates) == 0:
        return True
    else:
        return bad_updates


# RULE BUILDERS (by event)

# update = replace or add, SAME THING

