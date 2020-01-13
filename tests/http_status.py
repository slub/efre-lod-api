import requests

import lod_api


class HttpStatusBase:

    host = None

    def setup(self):
        from lod_api.main import read_config
        if len(lod_api.__path__) == 1:
            read_config(lod_api.__path__[0] + "/../apiconfig.json")

        self.host = "http://{host}:{port}".format(
            host=lod_api.CONFIG.get("debug_host"),
            port=lod_api.CONFIG.get("debug_port"),
        )
        # TODO: run app

    def _http_response(self, path, status_code=200, host=None):
        """ Prepends host to path and queries the url expecting
            a status code of `status_code` for the test to 
            succeed (assert)
        """
        if host:
            url = host + path
        else:
            url = self.host + path
        print(url)
        res = requests.get(url)
        print("expected: {}; got: {}"
              .format(status_code, res.status_code)
              )
        assert(res.status_code == status_code)
