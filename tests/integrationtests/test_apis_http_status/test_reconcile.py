import pytest
import requests

from ..http_status import HttpStatusBase


@pytest.mark.integration
@pytest.mark.httpstatus
@pytest.mark.reconcile
class TestReconcileHttpStatus(HttpStatusBase):
    host = None                                    # set in HttpStatusBase

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

    @pytest.mark.parametrize("endpt", ["/flyout/entity", "/flyout/property",
                                       "/flyout/type"])
    def test_endpoint_flyout(self, endpt, entity, test_count):
        """ Test every flyout endpoint for every entity index available. """

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

            if endpt in ["/flyout/entity", "/flyout/property"]:
                url = "/reconcile{endpt}?id={entity}/{id_}".format(
                          endpt=endpt, entity=entity, id_=id_)
                self._http_response(url)

    def test_endpoint_flyout_type(self, index_URI):
        # request every index-URI as type
        url = "/reconcile/flyout/type?id={id_}".format(id_=index_URI)
        self._http_response(url)

    @pytest.mark.parametrize("endpt", ["/suggest/entity", "/suggest/property",
                                       "/suggest/type"])
    def test_endpoint_suggest(self, endpt):
        """ Test very suggest endpoint provided."""
        url = "/reconcile{endpt}?prefix={search}".format(endpt=endpt,
                                                         search="randm string")
        self._http_response(url)


if __name__ == '__main__':
    pytest.main()
