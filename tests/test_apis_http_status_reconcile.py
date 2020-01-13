import pytest
import requests
import json

import lod_api
from .http_status import HttpStatusBase


class TestReconcileHttpStatus(HttpStatusBase):
    host = None                                    # set in HttpStatusBase
    ep_suggest = ["/suggest/entity", "/suggest/property", "/suggest/type"]

    def test_endpoint_reconcile(self):
        self._http_response("/reconcile")


    def test_endpoint_properties(self):
        """ Test HTTP response for the /reconcile/properties endpoint
            Things needed for the request:
              - type (GET) - 
              - limit (GET) - an integer
              - callback (optional, GET)
        """
        self._http_response("/reconcile/properties")


    def test_endpoint_flyout(self):
        """ Test every flyout endpoint for every index available. """
        test_count = 1

        for index in lod_api.CONFIG.get("indices_list"):
            # request to get id from one dataset
            search_url = self.host + "/{entity}/search".format(entity=index)
            print(search_url)
            search_res = requests.get(search_url)

            # get first dataset
            for res_json in json.loads(search_res.content)[0:test_count]:
                # get ID of first dataset (without rest of URI)
                id_ = res_json["@id"].split("/")[-1]

                for endpt in ["/flyout/entity", "/flyout/property"]:
                    url = "/reconcile{endpt}?id={entity}/{id_}".format(endpt=endpt, 
                          entity=index, id_=id_)
                    self._http_response(url)

        # request every index-URI
        for index in lod_api.CONFIG.get("indices").keys():
            url = "/reconcile/flyout/type?id={id_}".format(id_=index)
            self._http_response(url)

    def test_endpoint_suggest(self):
        """ Test very suggest endpoint provided."""
        for endpoint in self.ep_suggest:
            url = "/reconcile{ep}?prefix={search}".format(ep=endpoint,
                    search="random string")
            self._http_response(url)


if __name__ == '__main__':
    pytest.main()
