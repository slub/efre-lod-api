import pytest
import requests
import json

import lod_api


class TestResponse:
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

    def test_doc(self):
        res = requests.get(self.host + "{url}".format(
            url=lod_api.CONFIG.get("doc_url")
        ))
        assert(res.status_code == 200)

    def test_search(self):
        res = requests.get(self.host + "/search")
        assert(res.status_code == 200)

    def test_entity_search_index(self):
        """ search for get one dataset and its ID for each entity index and
            request this dataset directly via its ID"""
        for _, index in lod_api.CONFIG.get("indices").items():
            # request to get id from one dataset
            search_url = self.host + "/{entity}/search".format(entity=index["index"])
            print(search_url)
            search_res = requests.get(search_url)

            # get first dataset
            res_json = json.loads(search_res.content)[0]

            # get ID of first dataset (without rest of URI)
            id_ = res_json["@id"].split("/")[-1]

            url = self.host + "/{entity}/{id_}".format(entity=index["index"], id_=id_)
            print(url)
            res = requests.get(url)

            assert(res.status_code == 200)

    def test_source(self):
        # TODO
        pass

    def test_authority_provider(self):
        # TODO
        pass

    def test_non_existing_search(self):
        res = requests.get(self.host + "/bullshit/search")
        assert(res.status_code == 404)


if __name__ == '__main__':
    pytest.main()
