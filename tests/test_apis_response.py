import pytest
import requests

import lod_api


class TestResponse:
    host = None
    api_thread = None

    def setup(self):
        from lod_api.main import read_config, run_app
        if len(lod_api.__path__) == 1:
            read_config(lod_api.__path__[0] + "/../apiconfig.json")

        self.host = "http://{host}:{port}".format(
            host=lod_api.CONFIG.get("debug_host"),
            port=lod_api.CONFIG.get("debug_port"),
        )
        # TODO: run app
        # self.api_thread = threading.Thread(target=run_app(), args=())
        # self.api_thread.daemon = True
        # self.api_thread.start()

    def test_doc(self):
        res = requests.get(self.host + "{url}".format(
            url=lod_api.CONFIG.get("doc_url")
        ))
        assert(res.status_code == 200)

    def test_search(self):
        res = requests.get(self.host + "/search")
        assert(res.status_code == 200)

    def test_entity_search(self):
        for _, index in lod_api.CONFIG.get("indices").items():
            url = self.host + "/{entity}/search".format(entity=index["index"])
            print(url)
            res = requests.get(url)
            assert(res.status_code == 200)

    def test_non_existing_search(self):
        res = requests.get(self.host + "/bullshit/search")
        assert(res.status_code == 404)


if __name__ == '__main__':
    pytest.main()
