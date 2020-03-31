import yaml


class ConfigParser:
    def __init__(self, conf_fname):
        with open(conf_fname) as stream:
            self.conf = yaml.safe_load(stream)

        self.conf["indices_list"] = self._get_indices()
        self.conf["authorities_list"] = self._get_list_by_string("authorities")
        self.conf["sources_list"] = self._get_list_by_string("source_indices")

    def _get_indices(self):
        ret = set()
        for obj in [x for x in self.conf["indices"].values()]:
            ret.add(obj.get("index"))
        return list(ret)

    def _get_list_by_string(self, string_key):
        if self.conf.get(string_key):
            ret = set()
            for obj in self.conf[string_key]:
                ret.add(obj)
            return list(ret)
        else:
            return None

    def get(self, *config_attributes):
        ret = []
        for attr in config_attributes:
            if self.conf.get(attr):
                ret.append(self.conf.get(attr))
            else:
                print("config item not defined: \"{}\"".format(attr))

        if len(ret) == 1:
            return ret[0]
        elif len(ret) > 1:
            return ret
        else:
            return None
