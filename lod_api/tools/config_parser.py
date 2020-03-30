import yaml


class ConfigParser:
    def __init__(self, conf_fname):
        with open(conf_fname) as stream:
            self.conf = yaml.safe_load(stream)

        self.conf["indices_list"] = self._get_indices()
        self.conf["authorities_list"] = self._get_authorities()
        self.conf["sources_list"] = self._get_sources()

    def _get_indices(self):
        ret = set()
        for obj in [x for x in self.conf["indices"].values()]:
            ret.add(obj.get("index"))
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
