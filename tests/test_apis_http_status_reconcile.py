import pytest

from .http_status import HttpStatusBase


class TestReconcileHttpStatus(HttpStatusBase):
    host = None
    ep = ["/properties", "/suggest/entity", "/suggest/property",
          "/suggest/type"]
    ep_flyout = ["/flyout/entity", "/flyout/property", "/flyout/type"]

    def test_endpoint_reconcile(self):
        self._http_response("/reconcile")

    def test_endpoint_flyout(self):
        for endpt in self.ep_flyout:
            url = "/reconcile{endpt}?id={id_}".format(endpt=endpt, id_="persons/035143681")
            self._http_response(url)


if __name__ == '__main__':
    pytest.main()
