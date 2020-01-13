import json


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


class ConfigParser:
    def __init__(self, conf_fname):
        with open(conf_fname) as conf_file:
            self.conf = json.load(conf_file)

        self.conf["indices_list"] = self._get_indices()
        self.conf["authorities_list"] = self._get_authorities()
        self.conf["sources_list"] = self._get_sources()

    def _get_indices(self):
        ret = set()
        for obj in [x for x in self.conf["indices"].values()]:
            ret.add(obj.get("index"))
        # BUG Last Element doesn't work. So we add a whitespace Element
        # which won't work, instead of an Indexname
        #
        # See github issue:
        #   https://github.com/noirbizarre/flask-restplus/issues/695
        return list(ret)

    def _get_authorities(self):
        ret = set()
        for obj in self.conf["authorities"]:
            ret.add(obj)
        return list(ret)

    def _get_sources(self):
        ret = set()
        for obj in self.conf["source_indices"]:
            ret.add(obj)
        return list(ret)

    def get(self, *config_attributes):
        ret = []
        for attr in config_attributes:
            ret.append(self.conf.get(attr))

        if len(ret) == 1:
            return ret[0]
        elif len(ret) > 1:
            return ret
        else:
            return None
