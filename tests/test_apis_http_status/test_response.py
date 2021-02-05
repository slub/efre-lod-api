import pytest
import requests

from lod_api.tools.response import Response
from ..http_status import HttpStatusBase


response = Response(api=None)
file_ext = response.format.keys()


@pytest.mark.response
class TestResponse(HttpStatusBase):

    def setup(self):
        super().setup()
        # response = Response(api=None)
        # self.ending = response.format.keys()

    @pytest.mark.parametrize("ext", file_ext)
    def test_entity_index_type(self, entity, ext, test_count):
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

            self._http_response("/{entity}/{id_}.{ext}"
                                .format(entity=entity,
                                        id_=id_,
                                        ext=ext))


if __name__ == '__main__':
    pytest.main()
