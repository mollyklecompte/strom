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


def create_template_dstream(strm_nm, src_key, measures: list, uids: list, events: list, dparam_rules: list, filters: list, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, tags=None, fields=None):
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

def build_data_rules(mapping_list, puller=None, pull_args=None, pull_kwargs=None):
    # PUT STUFF HERE
    pass


def build_template(strm_nm, src_key, measure_rules: list, uids: list, filters: list, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, tags=None, fields=None, source_mapping_list=None, dstream_mapping_list = None):
    # measure_rules is list of tuples of form
    # ([measure name, measure type], [filtered measures], [dparams], [event_tups])
    # filter tuples: ('name', {inputs})
    # dparam tuples: ('name', {inputs}, {'target_measure': 'measure'})
    # event_tups are tuples of form (event name, {kwargs}, [measure1, filtered_measure2])
    # for example:
    # (['location', 'geo'], ['buttery_location'], [('turn', {kwargs})])

    measure_list = [msr for rule in measure_rules if type(rule) is tuple and len(rule) == 4 for msr in rule[0]]

    print("MEASURE LIST \n", measure_list)

    # build filters
    filter_list = [filter_builder.build_rules_dict(f[0], f[1]) for f in filters for m in f[1]['measure_list'] if m in [msr[0] for msr in measure_list]]
    print('FILTERED LIST: ', filter_list)

    # all filtered measures
    filtered_measures = [f"{m}{f['param_dict']['filter_name'][2]}" for f in filter_list for m in f['measure_list']]

    event_list = []
    dparam_list = []
    print("FILTERED MEASURES: ", filtered_measures)

    # if measure in measure_rules is filtered, filtered measure added to list at index 1
    for rule in measure_rules:
        rule = list(rule)
        rule[1] = [(f, m[1]) for f in filtered_measures for m in rule[0] if m[0] in f]

        print("Rule: ", rule)

        if len(rule[3]):
            for event in rule[3]:
                event_build_measures = [msr if m == msr[0] else f if m ==f[0] else None for m in event[2] for msr in rule[0] for f in rule[1]]
                print("BUILD MEASURES \n", event_build_measures )

                if not None in event_build_measures:
                    rule_set = build_rules_from_event(event[0], *event_build_measures, **event[1])
                    event_list.append(rule_set['event_rules'])
                    dparam_list.extend(rule_set['dparam_rules'])
                else:
                    raise ValueError("ya goofed")

        if len(rule[2]):
            for dp in rule[2]:
                dp_build_measures = [(m, msr) if v == msr[0] else (m, f) if v == f[0] else None for m, v in dp[2].items() for msr in rule[0] for f in rule[1]]

                if not None in dp_build_measures:
                    for bm in dp_build_measures:
                        dp[1][bm[0]] = bm[1]
                    dparam_list.append(dp_builder(dp[0], **dp[1]))
                else:
                    raise ValueError("ya goofed i say")


            # all_rules = [build_rules_from_event(
            #     e[0], [(msr[0], msr[1]) for msr in rule], **e[1]) for e in rule[4]]
            # for rules in all_rules:
            #     event_list.append(rules['event_rules'])
            #     dparam_list.extend(rules['dparam_rules'])


    template = create_template_dstream(strm_nm, src_key, measure_list, uids, event_list, dparam_list, filter_list, storage_rules, ingest_rules, engine_rules, foreign_keys, tags, fields)
    map_list = build_mapping(source_mapping_list, dstream_mapping_list)
    # NEED STUFF HERE
    # template.add_data_rules(map_list)
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

