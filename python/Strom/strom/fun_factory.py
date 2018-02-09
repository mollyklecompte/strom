from copy import deepcopy
from strom.fun_factory_children import *
from strom.dstream.dstream import DStream



update_guide = {
    'stream_name': {
        'new': {
            'function': update_stream_name,
            'field_key_arg': False,
            'args': ['new_name',]
        }
    },
    'user_description': {
        'new': {
            'function': update_description,
            'field_key_arg': False,
            'args': ['new_description'],
        }
    },
    'source_key': {
        'new': {
            'function': update_source_key,
            'field_key_arg': False,
            'args': ['new_source_key'],
        }
    },
    'user_ids': {
        'new': {
            'function': update_user_id,
            'field_key_arg': False,
            'args': ['new_id'],
        },
        'remove': {
            'function': prune_key,
            'field_key_arg': True,
            'args': ['type_key', 'remove_key'],
        }
    },
    'fields': {
        'new': {
            'function': update_field,
            'field_key_arg': False,
            'args': ['new_field'],
        },
        'remove': {
            'function': prune_key,
            'field_key_arg': True,
            'args': ['type_key', 'remove_key'],
        }
    },
    'tags': {
        'new': {
            'function': update_tag,
            'field_key_arg': False,
            'args': ['tag_name'],
        },
        'remove': {
            'function': prune_key,
            'field_key_arg': True,
            'args': ['type_key', 'remove_key'],
        }
    },
    'foreign_keys': {
        'new': {
            'function': update_foreign_key,
            'field_key_arg': False,
            'args': ['fk'],
        },
        'remove': {
            'function': prune_list_key,
            'field_key_arg': True,
            'args': ['type_key', 'remove_key'],
        }
    },
    'storage_rules': {
        'modify': {
            'function': update_rules,
            'field_key_arg': True,
            'args': ['rule_tups']
        }
    },
    'ingest_rules': {
        'modify': {
            'function': update_rules,
            'field_key_arg': True,
            'args': ['rule_tups']
        }
    },
    'engine_rules': {
        'modify': {
            'function': update_rules,
            'field_key_arg': True,
            'args': ['rule_tups']
        }
    },
    'measures': {
        'new': {
            'function': new_measure,
            'field_key_arg': False,
            'args': ['measure_tup']
        },
        'remove': {
            'function': prune_key,
            'field_key_arg': True,
            'args': ['type_key', 'remove_key'],
        }
    },
    'filters': {
        'new': {
            'function': new_filter,
            'field_key_arg': False,
            'args': ['filter'],
        },
        'modify': {
            'function': modify_filter,
            'field_key_arg': False,
            'args': ['filter_id', 'filter_param_tups'],
        },
        'remove': {
            'function': remove_transform,
            'field_key_arg': True,
            'args': ['type_key', 'transform_id']
        }
    },
    'dparam_rules': {
        'new': {
            'function': new_dparam(),
            'field_key_arg': False,
            'args': ['dparam'],
        },
        'modify': {
            'function': modify_dparam,
            'field_key_arg': False,
            'args': ['dparam_id', 'dparam_param_tups'],
        },
        'remove': {
            'function': remove_transform,
            'field_key_arg': True,
            'args': ['type_key', 'transform_id']
        }
    },
    'event_rules': {
        'new': {
            'function': new_event,
            'field_key_arg': False,
            'args': ['new_event_key', 'new_event_dict']
        },
        'modify': {
            'function': modify_event,
            'field_key_arg': False,
            'args': ['event_key', 'event_param_tups']
        },
        'remove': {
            'function': prune_key,
            'field_key_arg': True,
            'args': ['type_key', 'remove_key'],
        }
    }

}


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
        return 'invalid update', valid_update(template)



def valid_update(template: DStream):
    bad_updates = []

    # possible names of filtered measures
    filtered_measure_names = [
        '{}{}'.format(m, f_name)
        for m in template['measures'].keys()
        for f_name in [
            f['param_dict']['filter_name']
            for f in template['filters']
        ]
    ]

    # measure required by filter missing
    bad_updates.extend(
        [('filter', f['transform_name'], 'measure', m)
         for f in template['filters']
         for m in f['measure_list']
         if m not in template['measures'].keys() and m != 'timestamp']
    )

    # measure required by derived param missing
    bad_updates.extend(
        [
            ('derived param', dp['transform_name'], 'measure', m)
            for dp in template['dparam_rules']
            for m in dp['measure_list'] if m not in template['measures'].keys()
                                           and m not in filtered_measure_names
                                           and m != 'timestamp'
        ]
    )

    # PLACEHOLDER - DEAL WITH FILTER NAMES -___-

    bad_updates.extend(
        [('event', event_key, 'derived param', m)
            for event_key, event_dict in template['event_rules'].items()
            for m in event_dict["measure_list"]
            if m not in [
            dp['param_dict']['measure_rules']['output_name'] for dp in template['dparam_rules']] and m not in filtered_measure_names and m not in template['measures'].keys() and m != 'timestamp'
        ]
    )

    if len(bad_updates) == 0:
        return True
    else:
        return bad_updates


# RULE BUILDERS (by event)

# update = replace or add, SAME THING

