def isint(num):
    try:
        int(num)
        return True
    except (ValueError, TypeError):
        return False


def getNestedJsonObject(record, attribut_string):
    attr_list = attribut_string.split(">")
    if len(attr_list) == 1 and attr_list[0] in record:
        return record.get(attr_list[0])
    elif len(attr_list) == 1 and isint(attr_list[0]) and isinstance(record, list):
        return record[int(attr_list[0])]
    elif len(attr_list) > 1 and isint(attr_list[0]) and isinstance(record, list):
        return getNestedJsonObject(record[int(attr_list[0])], ">".join(attr_list[1:]))
    elif len(attr_list) > 1 and attr_list[0] in record:
        return getNestedJsonObject(record[attr_list[0]], ">".join(attr_list[1:]))
    else:
        return None


def get_fields_with_subfields(prefix, data):
    for k, v in data.items():
        yield prefix + k
        if "properties" in v:
            for item in get_fields_with_subfields(k + ".", v["properties"]):
                yield item


def es_wrapper(es, action, index, body=None, doc_type=None, id=None, size=None, from_=None, _source_exclude=None, _source_include=None):
    server_version = int(es.info()['version']['number'][0])
    if action == "search":
        if server_version < 7:
            return es.search(index=index, body=body, size=size, from_=from_, _source_exclude=_source_exclude, _source_include=_source_include)
        elif server_version >= 7:
            return es.search(index=index, body=body, size=size, from_=from_, _source_excludes=_source_exclude, _source_includes=_source_include)
    elif action == "get":
        if server_version < 7:
            return es.get(index=index, doc_type=doc_type,  id=id, _source_exclude=_source_exclude, _source_include=_source_include)
        elif server_version >= 7:
            return es.get(index=index, doc_type=doc_type,  id=id, _source_excludes=_source_exclude, _source_includes=_source_include)
