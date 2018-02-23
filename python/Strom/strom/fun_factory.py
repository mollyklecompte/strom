from copy import deepcopy

from strom.data_map_builder import build_mapping
from strom.dstream.dstream import DStream
from strom.fun_update_guide import update_guide
from strom.rules_dict_builder import event_builder

__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"


def build_rules_dicts(event: str, base_measure: tuple, **kwargs):
    # event: str, event type (key in event_builder)
    # base_measure: tuple, name + type i.e. ('location', 'geo')
    base_event = event_builder[event]
    if base_measure[1] != base_event['base_measure_type']:
        raise ValueError (f"Event {base_event} requires base measure type {base_event['base_measure_type']} Provided base measure type {base_measure[1]}")
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
        rules = fn(base_measure[0], partition_list, stream_id, **inputs)

        return rules


def create_template_dstream(strm_nm, src_key, measures: list, uids: list, events: list, dparam_rules: list, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, filters=None, tags=None, fields=None):
    # measures: list of tuples (measure_name, dtype)

    template = DStream()
    template['stream_name'] = strm_nm
    template['source_key'] = src_key
    template.add_measures(measures)  # expects list of tuples (measure, dtype)
    template.add_user_ids(uids)
    template['user_description'] = usr_dsc

    template.add_dparams(dparam_rules)
    template.add_events(events)

    if filters is not None:
        template.add_filters(filters)

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


def build_template(strm_nm, src_key, measure_rules: list, uids: list,  usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, tags=None, fields=None, source_mapping_list=None, dstream_mapping_list = None):
    # measure_rules is list of tuples of form
    # (measure name, measure type, [filtered measures], [event_tups]
    # event_tups are tuples of form (event name, {kwargs})
    # for example:
    # ('location', 'geo', ['buttery_location'], [('turn', {kwargs})])

    measure_list = []
    event_list = []
    dparam_list = []

    for m in measure_rules:
        if type(m) is tuple and len(m) == 4:
            measure_list.append((m[0], m[1]))
            all_rules = [build_rules_dicts(
                e[0], (m[0], m[1]), **e[1]) for e in m[3]]
            for rules in all_rules:
                event_list.append(rules['event_rules'])
                dparam_list.extend(rules['dparam_rules'])
        else:
            raise TypeError('Measure rules must be len 4 tuple')

    template = create_template_dstream(strm_nm, src_key, measure_list, uids, event_list, dparam_list, usr_dsc, storage_rules, ingest_rules, engine_rules, foreign_keys, tags, fields)
    map_list = build_mapping(source_mapping_list, dstream_mapping_list)
    template.add_mapping(map_list)
    return template


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

    # measure required by derived param missing
    bad_updates.extend([('derived param', dp['transform_name'], 'measure', m) for dp in template['dparam_rules'] for m in dp['measure_list'] if m not in template['measures'].keys() and m not in possible_filtered_measures and m != 'timestamp'])

    bad_updates.extend([('event', event_key, 'derived param', m) for event_key, event_dict in template['event_rules'].items() for m in event_dict["measure_list"] if m not in [dp['param_dict']['measure_rules']['output_name'] for dp in template['dparam_rules']] and m not in possible_filtered_measures and m not in template['measures'].keys() and m != 'timestamp'])

    if len(bad_updates) == 0:
        return True
    else:
        return bad_updates


# RULE BUILDERS (by event)

# update = replace or add, SAME THING

