import pytest

from ..http_status import HttpStatusBase
from ..mock_data import MockDataHandler


md_handler = MockDataHandler()

@pytest.mark.integration
@pytest.mark.mockdata
@pytest.mark.response
class TestMockDataResponseFormat(HttpStatusBase):
    @pytest.mark.parametrize("frmt", ["", "json", "jsonl", "ttl"])
    @pytest.mark.parametrize("endp_req", [
        "/resources/1358199159",
        ])
    def test_mock_format(self, endp_req, frmt, overwrite_mock_output):
        """ Query Search endpoint."""
        if not frmt:
            frmt = "json"
            res = self._http_response(endp_req)
            fname = type(self).__name__ + endp_req
        else:
            res = self._http_response(endp_req, get_param={"format": frmt})
            fname = type(self).__name__ + endp_req + "-" + frmt 


        if overwrite_mock_output:
            md_handler.write(fname, res.text, format=frmt)

        md_handler.compare(fname, res.text, format=frmt)

        

if __name__ == '__main__':
    pytest.main()
