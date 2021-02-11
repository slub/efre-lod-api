import pytest
from lod_api.apis.search_and_access import *

class Elasticmock:
    def __init__(self, *args, **kwargs):
        pass

    def info(self):
        return {"version": {"number": [7]}}

    def search(self, *args, **kwargs):
        resp = {
                "hits": {
                    "hits": [
                        {"_source": {"@id": "1", "content":  "first_hit"}},
                        {"_source": {"@id": "2", "content": "second_hit"}}
                        ]
                    }
                }
        return resp


@pytest.mark.this
@pytest.mark.unit
@pytest.mark.api_search
def test_search(client, monkeypatch):
    monkeypatch.setattr(elasticsearch, "Elasticsearch", Elasticmock)
    response = client.get("/search")

    # Validate the response
    assert response.status_code == 200
    assert response.json == [
            {"@id": "1", "content":  "first_hit"},
            {"@id": "2", "content": "second_hit"}
            ]

