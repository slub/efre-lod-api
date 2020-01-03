import json


def get_fields_with_subfields(prefix, data):
    for k, v in data.items():
        yield prefix + k
        if "properties" in v:
            for item in get_fields_with_subfields(k+".", v["properties"]):
                yield item


class ConfigParser:
    def __init__(self, conf_fname):
        with open(conf_fname) as conf_file:
            self.conf = json.load(conf_file)

        self.conf["indices_list"] = self._get_indices()
        self.conf["authorities_list"] = self._get_authorities()

    def _get_indices(self):
        ret = set()
        for obj in [x for x in self.conf["indices"].values()]:
            ret.add(obj.get("index"))
        # BUG Last Element doesn't work. So we add a whitespace Element
        # which won't work, instead of an Indexname
        #
        # See github issue:
        #   https://github.com/noirbizarre/flask-restplus/issues/695
        return list(ret) + [" "]

    def _get_authorities(self):
        ret = set()
        for obj in self.conf["authorities"]:
            ret.add(obj)
        # BUG: see above
        return list(ret) + [" "]

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
