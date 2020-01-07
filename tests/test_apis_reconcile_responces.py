import pytest
import requests

import lod_api


class TestReconcileResponse:
    host = None
    endpoints = ["/", "/flyout/entity", "/flyout/property", "/flyout/type"
                 "/properties", "/suggest/entity", "/suggest/property",
                 "/suggest/type"]

    def setup(self):
        from lod_api.main import read_config
        if len(lod_api.__path__) == 1:
            read_config(lod_api.__path__[0] + "/../apiconfig.json")

        self.host = "http://{host}:{port}".format(
            host=lod_api.CONFIG.get("debug_host"),
            port=lod_api.CONFIG.get("debug_port"),
        )

    def test_endpoints(self):
        for endpoint in self.endpoints:
            url = self.host + "/reconcile{}".format(endpoint)
            print(url)
            res = requests.get(url)
            assert(res.status_code == 200)


if __name__ == '__main__':
    pytest.main()
