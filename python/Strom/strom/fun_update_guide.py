from strom.fun_factory_children import *


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
            'function': new_dparam,
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