import pytest
import requests

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
            if not search_res.ok:
                raise Exception("Could not retrieve result for URL=\'{}\'"
                                .format(search_url))

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
            if not search_res.ok:
                raise Exception("Could not retrieve result for URL=\'{}\'"
                                .format(search_url))

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

        url_schema = lod_api.CONFIG.get("authorities")

        for authority in url_schema:
            for entity in lod_api.CONFIG.get("indices_list"):
                # request to get id from one dataset
                search = url_schema[authority]
                search_url = (self.host
                              + "/{entity}/search?q=sameAs.@id:\"{search}\""
                              .format(entity=entity, search=search))

                print(search_url)
                search_res = requests.get(search_url)
                if not search_res.ok:
                    raise Exception("Could not retrieve result for URL=\'{}\'"
                                    .format(search_url))

                for res_json in search_res.json()[0:test_count]:
                    # get ID of first dataset (without rest of URI)

                    auth_id = None
                    for item in res_json["sameAs"]:
                        if url_schema[authority] in item["@id"]:
                            print("authority-item: ", item["@id"])
                            # hence we've configured the IDs in the config, we split
                            # the item with the base_uri and get the ID with the 2nd slice
                            auth_id = item["@id"].split(url_schema[authority])[1]
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
