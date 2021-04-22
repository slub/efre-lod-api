import pytest
from lod_api.apis.explore import *

class Elasticmock:
    def __init__(self, *args, **kwargs):
        """ define basic responses """
        self.topicsearch_resp = {
                "hits": {
                    "hits": [
                        {
                        "_score": 1.0,
                        "_source": {
                            "@id": "https://data.slub-dresden.de/topics/1111111",
                            "preferredName": "first_hit",
                            "alternateName": ["First Hit"],
                            "description": "Beschreibung 1",
                            "additionalType": []
                            }
                        },
                        {
                        "_score": 0.9,
                        "_source": {
                            "@id": "https://data.slub-dresden.de/topics/2222222",
                            "preferredName": "second_hit",
                            "alternateName": ["Second Hit"],
                            "description": "Beschreibung 2",
                            "additionalType": [
                                {"@id": "https://data.slub-dresden.de/topics/1234567",
                                 "name": "Topicname 1",
                                 "description": "nice topic 1"},
                                {"name": "Topicname 2",
                                 "description": "topic without @id"}
                                ]
                            }
                        }
                        ]
                    }
                }
        self.aggregate_topics_strict_resp = {
                "hits": {
                    "total": {
                        "value": 42
                        }
                    },
                "aggregations": {
                    "topAuthors": {
                        "buckets": [
                            {"key": "Karl", "doc_count": 12},
                            {"key": "Orff", "doc_count": 10}
                            ]
                        },
                    "mentions": {
                        "buckets": [
                            {"key": "Musik", "doc_count": 12},
                            {"key": "Kantate", "doc_count": 10}
                            ]
                        },
                    "datePublished": {
                        "buckets": [
                            {"key_as_string": "1937-06-08", "doc_count": 12},
                            ]
                        },
                    }
                }

    def info(self):
        return {"version": {"number": [7]}}

    def search(self, *args, **kwargs):
        if kwargs.get("body"):
            if not type(kwargs["body"]) == dict:
                raise Exception("search body should be of type dict")
        if (kwargs["body"].get("query") and
                kwargs["body"]["query"].get("simple_query_string")):
            # simple topic query detected by simple_query_string
            return self.topicsearch_resp
        if (kwargs["body"].get("aggs") and
                kwargs["body"]["aggs"].get("topAuthors") and
                kwargs["body"]["aggs"].get("mentions") and
                kwargs["body"]["aggs"].get("datePublished")):
            # topics aggregate strict detected (used with scroll
            # api to get correct hits value)
            resp = self.aggregate_topics_strict_resp.copy()
            resp["hits"]["total"]["value"] = 10001
            return resp
        else:
            raise NotImplementedError("search test not implemented")

    def msearch(self, *args, **kwargs):
        # body must include a even number of json objects, thus
        # the count of \n in the serialized string is odd
        assert kwargs["body"].strip().count("\n") % 2 == 1

        queries = [json.loads(q) for q in kwargs["body"].split("\n")]

        responds = []
        for i, q in enumerate(queries):
            # identify topics aggregate strict query
            if (q.get("aggs") and
                    q["aggs"].get("topAuthors") and
                    q["aggs"].get("mentions") and
                    q["aggs"].get("datePublished")):
                resp = self.aggregate_topics_strict_resp.copy()

                if len(responds) == 0:
                    # manipulate hits to hit maximal value
                    resp["hits"]["total"]["value"] = 10000
                responds.append(resp)
            elif q == {}:
                continue
            else:
                raise NotImplementedError("msearch test not implemented")

        return {"responses": responds}



@pytest.mark.unit
@pytest.mark.api_explore
def test_topicsearch_get(client, monkeypatch):
    monkeypatch.setattr(elasticsearch, "Elasticsearch", Elasticmock)
    response = client.get("/explore/topicsearch?q=query")
    # response = client.get("/search")

    # Validate the response
    assert response.status_code == 200
    resp = response.json

    # check translation of keys
    # be carefull as the docCount is altered
    # from the original value of 42
    assert resp[0] == {
            'additionalTypes': [],
            'alternateName': ['First Hit'],
            'description': 'Beschreibung 1',
            'id': 'https://data.slub-dresden.de/topics/1111111',
            'name': 'first_hit',
            'score': 1.0,
            'aggregations': {
                'datePublished': [{'count': 12, 'year': 1937}],
                'docCount': 10001,
                'mentions': [{'docCount': 12, 'name': 'Musik'} ,{'docCount': 10, 'name': 'Kantate'}],
                'topAuthors': [{'doc_count': 12, 'key': 'Karl'}, {'doc_count': 10, 'key': 'Orff'}]
                }
            }

    # check additionalType without @id
    assert resp[1]["additionalTypes"][1] == {
            'name': 'Topicname 2',
            'description': 'topic without @id'
            }

@pytest.mark.unit
@pytest.mark.api_explore
def test_topicsearch_post(client, monkeypatch):
    monkeypatch.setattr(elasticsearch, "Elasticsearch", Elasticmock)
    data = {
            'size': 15,
            'query': {
                'simple_query_string': {
                    'query': 'query',
                    'fields': ['preferredName',
                               'alternateName',
                               'description',
                               'additionalType.description',
                               'additionalType.name'
                               ],
                'default_operator': 'and'}
                }
            }
    response = client.post("/explore/topicsearch", json={"body": data})
    # Validate the response
    assert response.status_code == 200
    resp = response.json
    assert len(resp) == 2


