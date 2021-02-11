import pytest
import requests

from ..http_status import HttpStatusBase

@pytest.mark.integration
@pytest.mark.httpstatus
class TestHttpStatusEndpoints(HttpStatusBase):
    @pytest.mark.api_frontend
    def test_doc(self, apiconfig):
        """ Query Documentation URL with swagger frontend."""
        self._http_response(path=apiconfig.get("frontend_url"))

    @pytest.mark.api_search
    def test_search(self):
        """ Query Search endpoint."""
        self._http_response("/search")

    @pytest.mark.api_search
    def test_entity_search_index(self, entity, test_count):
        """ search for get one dataset and its ID for each entity index and
            request this dataset directly via its ID
        """
        # request to get id from one dataset
        search_url = self.host + "/{entity}/search".format(entity=entity)
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
                                .format(entity=entity, id_=id_))

    @pytest.mark.api_source
    def test_source_index(self, source, test_count):
        """ search for get one dataset and its ID for each source index and
            request this dataset directly via its ID from the source
        """
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

    @pytest.mark.api_authority
    def test_authority_index(self, authority, entity, test_count):
        """ search for get one dataset and its ID for each source index and
            request this dataset directly via its ID from the source"""

        auth_key = authority[0]
        auth_url = authority[1]

        # request to get id from one dataset
        search_url = (self.host
                      + "/{entity}/search?q=sameAs.@id:\"{search}\""
                      .format(entity=entity, search=auth_url))

        print(search_url)
        search_res = requests.get(search_url)
        if not search_res.ok:
            # if there is no entity from this authority, we let the
            # test pass instead of raising an exception because not
            # all endpoints contain data from all authorities
            print("Could not retrieve result for URL={}".format(search_url))
            return

        for res_json in search_res.json()[0:test_count]:
            # get ID of first dataset (without rest of URI)

            auth_id = None
            for item in res_json["sameAs"]:
                if auth_url in item["@id"]:
                    print("authority-item: ", item["@id"])
                    # hence we've configured the IDs in the config, we split
                    # the item with the base_uri and get the ID with the 2nd slice
                    auth_id = item["@id"].split(auth_url)[1]
            if not auth_id:
                continue

            self._http_response("/{authority}/{entity}/{auth_id}"
                                .format(authority=auth_key,
                                        entity=entity,
                                        auth_id=auth_id)
                                )

    def test_non_existing_search(self):
        """ Query non-existing endpoint and expacting it to return 404."""
        self._http_response("/bullshit/search", status_code=404)


if __name__ == '__main__':
    pytest.main()
