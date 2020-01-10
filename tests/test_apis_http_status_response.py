import pytest
import requests
import json
import re

import lod_api
from lod_api.apis.response import Response
from .http_status import HttpStatusBase


class TestResponse(HttpStatusBase):

    def setup(self):
        super().setup()
        response = Response(api=None)
        self.ending = response.format.keys()

    def test_entity_index_type(self):
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
            for res_json in json.loads(search_res.content)[0:test_count]:
                # get ID of first dataset (without rest of URI)
                id_ = res_json["@id"].split("/")[-1]

                for type_ in self.ending:
                    self._http_response("/{entity}/{id_}.{type_}"
                              .format(entity=index,
                              id_=id_,
                              type_=type_))


if __name__ == '__main__':
    pytest.main()
