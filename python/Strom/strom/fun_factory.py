from strom.dstream.dstream import DStream

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


# update wrapper
def update_template(template_json):
    template = DStream()
    template.load_from_json(template_json)
    template['stream_token'] = template_json['stream_token']


# update = replace or add, SAME THING
def update_stream_name(template: DStream, new_name):
    template['stream_name'] = new_name


def update_source_key(template: DStream, new_key):
    template['source_key'] = new_key


def update_description(template: DStream, new_desc):
    template['user_description'] = new_desc


# update = add with replace option
def update_user_id(template: DStream, new_id: str, old_id=None):
    if old_id is not None:
        prune_key(template, 'user_ids', old_id)
    template.add_user_id(new_id)


def update_field(template: DStream, new_field: str, old_field=None):
    if old_field is not None:
        prune_key(template, 'fields', old_field)
    template.add_field(new_field)


def update_tag(template: DStream, tag_name: str, old_tag=None):
    if old_tag is not None:
        prune_key(template, 'tags', old_tag)
    template.add_tag(tag_name)


def update_foreign_key(template: DStream, fk: str, old_fk=None):
    if old_fk is not None:
        prune_key(template, 'foreign_keys', old_fk)
    template.add_fk(fk)


# update = edit only (SIMPLE RULES - fields, uids, foreign keys, tags)
def update_rules(template: DStream, rules_key: str, rule_tups: list):
    for tup in rule_tups:
        template[rules_key][tup[0]] = tup[1]


# update = true edit (TRANSFORM/ EVENT RULES)
def modify_filter(
        template: DStream,
        filter_id,
        filter_param_tups: list,
        new_partition_list=None,
        change_comparison=False):

    # ADD SOME SORT OF ERROR THROWING/HANDLING FOR NO/ MULTIPLE ID MATCH
    filter = [f for f in template['filters'] if f['transform_id'] == filter_id][0]
    for tup in filter_param_tups:
        filter['param_dict'][tup[0]] = tup[1]
    if new_partition_list is not None:
        filter['partition_list'] = new_partition_list
    if change_comparison is True:
        if filter['logical_comparison'] == 'AND':
            filter['logical_comparison'] = 'OR'
        else:
            filter['logical_comparison'] = 'AND'


def modify_dparam(
        template: DStream,
        dparam_id,
        dparam_param_tups: list,
        new_partition_list=None,
        change_comparison=False):
    # ADD SOME SORT OF ERROR THROWING/HANDLING FOR NO/ MULTIPLE ID MATCH
    dparam = [p for p in template['dparam_rules'] if p['transform_id'] == dparam_id][0]
    for tup in dparam_param_tups:
        dparam['param_dict'][tup[0]] = tup[1]
    if new_partition_list is not None:
        dparam['partition_list'] = new_partition_list
    if change_comparison is True:
        if dparam['logical_comparison'] == 'AND':
            dparam['logical_comparison'] = 'OR'
        else:
            dparam['logical_comparison'] = 'AND'


def modify_event(
        template: DStream,
        event_key,
        event_param_tups,
        new_partition_list=None,
        change_comparison=False):
    for tup in event_param_tups:
        template['event_rules'][event_key]['param_dict'][tup[0]] = tup[1]
    if new_partition_list is not None:
        template['event_rules'][event_key]['partition_list'] = new_partition_list

# update = add new (TRANSFORM RULES)
def new_filter(template: DStream, filter: dict):
    template['filters'].append(filter)


def new_dparam(template: DStream, dparam: dict):
    template['dparam_rules'].append(dparam)


# DELETE SECTION
def prune_key(template: DStream, type_key, remove_key):  # includes measures + events
    del template[type_key][remove_key]


def remove_transform(template: DStream, type_key, transform_id):
    for t in template[type_key]:
        if t['transform_id'] == transform_id:
            del t