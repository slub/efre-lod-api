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
        for index in lod_api.CONFIG.get("indices_list"):
            # request to get id from one dataset
            search_url = self.host + "/{entity}/search".format(entity=index)
            print(search_url)
            search_res = requests.get(search_url)

            # get first dataset
            res_json = json.loads(search_res.content)[0]

            # get ID of first dataset (without rest of URI)
            id_ = res_json["@id"].split("/")[-1]

            url = self.host + "/{entity}/{id_}".format(entity=index, id_=id_)
            print(url)
            res = requests.get(url)

            assert(res.status_code == 200)

    def test_source_index(self):
        """ search for get one dataset and its ID for each source index and
            request this dataset directly via its ID from the source"""
        for source in lod_api.CONFIG.get("sources_list"):
            # request to get id from one dataset
            search_url = (self.host
                          + "/search?q=isBasedOn:*\"{source}\""
                          .format(source=source))

            print(search_url)
            search_res = requests.get(search_url)

            # get first dataset
            res_json = json.loads(search_res.content)[0]

            # get ID of first dataset (without rest of URI)
            id_ = res_json["@id"].split("/")[-1]

            url = (self.host 
                   + "/source/{source}/{id_}"
                   .format(source=source, id_=id_))

            print(url)
            res = requests.get(url)

            assert(res.status_code == 200)

    def test_authority_index(self):
        """ search for get one dataset and its ID for each source index and
            request this dataset directly via its ID from the source"""


        url_schema = {"gnd": "d-nb.info/gnd",
                      "swb": "swb.bsz-bw.de",
                      "viaf": "viaf.org",
                      "wd": "wikidata"
                      }

        for authority in lod_api.CONFIG.get("authorities"):
            for entity in lod_api.CONFIG.get("indices_list"):
                # request to get id from one dataset
                search = url_schema[authority]
                search_url = (self.host
                              + "/{entity}/search?q=sameAs:*\"{search}\""
                              .format(entity=entity, search=search))

                print(search_url)
                search_res = requests.get(search_url)

         

                # get first dataset
                res_json = json.loads(search_res.content)[0]

                # get ID of first dataset (without rest of URI)
                # Problem: for each authority provider the URI looks different
                #          as well as the form of the ID
                import re
                pattern = re.compile("Q?[\d-]{6,}X?")
                auth_id = None
                for item in res_json["sameAs"]:
                    if url_schema[authority] in item:
                        print("authority-item: ", item)
                        auth_id = pattern.search(item).group()
                if not auth_id:
                    continue

                url = (self.host 
                       + "/{authority}/{entity}/{auth_id}"
                       .format(authority=authority, entity=entity, auth_id=auth_id))

                print(url)
                res = requests.get(url)

                assert(res.status_code == 200)

    def test_authority_provider(self):
        # TODO
        pass

    def test_non_existing_search(self):
        res = requests.get(self.host + "/bullshit/search")
        assert(res.status_code == 404)


if __name__ == '__main__':
    pytest.main()
