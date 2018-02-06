from strom.dstream.dstream import DStream

def create_template(strm_nm, src_key, measures: list, ids: list, events: list, dparam_rules: list, usr_dsc="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, filters=None, tags=None, fields=None):

    template = DStream()
    template['stream_name'] = strm_nm
    template['source_key'] = src_key
    template.add_measures(measures)
    template.add_user_ids(ids)
    template['user_description'] = usr_dsc
    template.add_events(events)
    template['dparam_rules'].extend(dparam_rules)

    if storage_rules is not None: # add else branch w default
        template['storage_rules'] = storage_rules

    if ingest_rules is not None: # add else branch w default
        template['ingest_rules'] = ingest_rules

    if engine_rules is not None: # add else branch w default
        template['engine_rules'] = engine_rules

    # the totally optional section
    if tags is not None: # is this really gonna be a dict?
        template.add_tags(tags)

    if fields is not None:
        template.add_fields(fields)

    if filters is not None:
        template['filters'].extend(filters)

    if foreign_keys is not None:
        template.add_foreign_keys(foreign_keys)
