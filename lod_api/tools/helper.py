import elasticsearch

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


class ES_wrapper:
    """ wraps functionality of the python elasticsearch client used in lod-api

        In Order to properly react on different elasticsearch versions
        this wrapper manages the difference in function calls to the es api
    """
    @staticmethod
    def call(es, action, **kwargs):
        """ Call a method of the elasticsearch api on a specified index
        with multiple variable kwargs as options to each call. """
        server_version = int(es.info()['version']['number'][0])
        client_version = elasticsearch.VERSION[0]
        if server_version < 7 and client_version < 7:
            if '_source_excludes' in kwargs:
                kwargs['_source_exclude'] = kwargs.pop('_source_excludes')
            if '_source_includes' in kwargs:
                kwargs['_source_include'] = kwargs.pop('_source_includes')
        return getattr(es, action)(**kwargs)

    @staticmethod
    def get_mapping_props(es, index, doc_type=None):
        """ Requests the properties of a mapping applied to one index """
        server_version = int(es.info()['version']['number'][0])
        mapping = es.indices.get_mapping(index=index)
        if server_version < 7 and doc_type:
            return mapping[index]["mappings"][doc_type]["properties"]
        elif server_version >= 7:
            return mapping[index]["mappings"]["properties"]
