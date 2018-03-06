from strom.dstream.dstream import DStream


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
        prune_list_key(template, 'foreign_keys', old_fk)
    template.add_fk(fk)


# update = edit only (SIMPLE RULES - storage, engine, ingest)
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
        dparam['param_dict']['func_params'][tup[0]] = tup[1]
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
        template['event_rules'][event_key]['param_dict']['event_rules'][tup[0]] = tup[1]
    if new_partition_list is not None:
        template['event_rules'][event_key]['partition_list'] = new_partition_list
    if change_comparison is True:
        if template['event_rules'][event_key]['logical_comparison'] == 'AND':
            template['event_rules'][event_key]['logical_comparison'] = 'OR'
        else:
            template['event_rules'][event_key]['logical_comparison'] = 'AND'


# update = add new (TRANSFORM RULES)
def new_filter(template: DStream, filter: dict):
    template['filters'].append(filter)


def new_dparam(template: DStream, dparam: dict):
    template['dparam_rules'].append(dparam)


def new_event(template: DStream, event_key, event_dict):
    template.add_event(event_key, event_dict)


def new_measure(template: DStream, measure_tup):
    template.add_measure(measure_tup[0], measure_tup[1])


# DELETE SECTION
def prune_key(template: DStream, type_key, remove_key):  # includes measures + events
    del template[type_key][remove_key]


def prune_list_key(template: DStream, type_key, remove_key):  # foreign keys
    template[type_key] = [i for i in template[type_key] if remove_key not in i.keys()]


def remove_transform(template: DStream, type_key, transform_id):
    template[type_key] = [i for i in template[type_key] if i['transform_id'] != transform_id]
