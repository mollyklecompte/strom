from copy import deepcopy
from strom.fun_factory_children import *
from strom.dstream.dstream import DStream
from strom.fun_update_guide import update_guide



def create_template(strm_nm, src_key, measures: list, uids: list, events: list, dparam_rules: list, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, filters=None, tags=None, fields=None):

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


class Update(dict):
    def __init__(self, field, type, *args, **kwargs):
        super().__init__()
        self['field']: field
        self['type']: type
        self['args']: args
        self['kwargs']: kwargs

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
