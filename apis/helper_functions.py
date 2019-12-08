import json


def get_fields_with_subfields(prefix, data):
    for k, v in data.items():
        yield prefix + k
        if "properties" in v:
            for item in get_fields_with_subfields(k+".", v["properties"]):
                yield item


def load_config(config, *config_attributes):
    ret = []
    with open(config) as data_file:
        cfg_file = json.load(data_file)
        for attr in config_attributes:
            ret.append(cfg_file.get(attr))
    if len(ret) == 1:
        return ret[0]
    elif len(ret) > 1:
        return ret
    else:
        return None


def get_indices():
    ret = set()
    with open("apiconfig.json") as data_file:
        config = json.load(data_file)
        for obj in [x for x in config["indices"].values()]:
            ret.add(obj.get("index"))
    # BUG Last Element doesn't work. So we add a whitespace Element
    # which won't work, instead of an Indexname
    #
    # See github issue:
    #   https://github.com/noirbizarre/flask-restplus/issues/695
    return list(ret) + [" "]


def get_authorities():
    ret = set()
    with open("apiconfig.json") as data_file:
        config = json.load(data_file)
        for obj in config["authorities"]:
            ret.add(obj)

    # BUG: See get_indices()
    return list(ret) + [" "]
