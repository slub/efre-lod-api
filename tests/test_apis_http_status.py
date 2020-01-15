import pytest
import requests
import re

import lod_api
from .http_status import HttpStatusBase


class TestHttpStatusEndpoints(HttpStatusBase):
    def test_doc(self):
        """ Query Documentation URL with swagger frontend."""
        self._http_response(path=lod_api.CONFIG.get("doc_url"))

    def test_search(self):
        """ Query Search endpoint."""
        self._http_response("/search")

    def test_entity_search_index(self):
        """ search for get one dataset and its ID for each entity index and
            request this dataset directly via its ID
        """
        test_count = 1

        for index in lod_api.CONFIG.get("indices_list"):
            # request to get id from one dataset
            search_url = self.host + "/{entity}/search".format(entity=index)
            print(search_url)
            search_res = requests.get(search_url)

            # get first dataset
            for res_json in search_res.json()[0:test_count]:
                # get ID of first dataset (without rest of URI)
                id_ = res_json["@id"].split("/")[-1]

                self._http_response("/{entity}/{id_}"
                                    .format(entity=index, id_=id_))

    def test_source_index(self):
        """ search for get one dataset and its ID for each source index and
            request this dataset directly via its ID from the source
        """
        test_count = 1

        for source in lod_api.CONFIG.get("sources_list"):
            # request to get id from one dataset
            search_url = (self.host
                          + "/search?q=isBasedOn:*\"{source}\""
                          .format(source=source))

            print(search_url)
            search_res = requests.get(search_url)

            # get first dataset
            for res_json in search_res.json()[0:test_count]:
                # get ID of first dataset (without rest of URI)
                id_ = res_json["@id"].split("/")[-1]

                self._http_response("/source/{source}/{id_}"
                                    .format(source=source, id_=id_))

    def test_authority_index(self):
        """ search for get one dataset and its ID for each source index and
            request this dataset directly via its ID from the source"""

        test_count = 1

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

                # get first dataset, or all
                for res_json in search_res.json()[0:test_count]:
                    # get ID of first dataset (without rest of URI)
                    # Problem: for each authority provider the URI looks different
                    #          as well as the form of the ID

                    # there is a wikidata id: http://www.wikidata.org/entity/Q54828
                    # which has five digits which seems to be the lower bound
                    pattern = re.compile("(Q[0-9]{1,}|[0-9-]{5,}X?)")
                    auth_id = None
                    for item in res_json["sameAs"]:
                        if url_schema[authority] in item:
                            print("authority-item: ", item)
                            auth_id = pattern.search(item).group()
                    if not auth_id:
                        continue

                    self._http_response("/{authority}/{entity}/{auth_id}"
                                        .format(authority=authority,
                                                entity=entity,
                                                auth_id=auth_id)
                                        )

    def test_non_existing_search(self):
        """ Query non-existing endpoint and expacting it to return 404."""
        self._http_response("/bullshit/search", status_code=404)


if __name__ == '__main__':
    pytest.main()
