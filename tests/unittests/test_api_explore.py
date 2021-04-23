import pytest
from copy import deepcopy
from lod_api.apis.explore import *
from lod_api.apis.explore_queries import (
        topic_query,
        topic_aggs_query_strict
        )


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
            resp = deepcopy(self.aggregate_topics_strict_resp)
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
                resp = deepcopy(self.aggregate_topics_strict_resp)

                if len(responds) == 0:
                    # manipulate hits to hit maximal value
                    # for the first response
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

    def check_this(response):
        """ Check resoonse from /topicsearch endpoint """
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
                }

        # check additionalType without @id
        assert resp[1]["additionalTypes"][1] == {
                'name': 'Topicname 2',
                'description': 'topic without @id'
                }
    # GET
    response = client.get("/explore/topicsearch?q=query")
    check_this(response)

    # POST
    query = topic_query("query", 15, ["preferredName", "alternateName"], [])
    response = client.post("/explore/topicsearch", json={"body": query})
    check_this(response)


@pytest.mark.unit
@pytest.mark.api_explore
def test_aggregations_get(client, monkeypatch):
    monkeypatch.setattr(elasticsearch, "Elasticsearch", Elasticmock)
    def check_this(response):
        """ Check response from /aggregations endpoint """

        data1 = {
               'datePublished': [{'count': 12, 'year': 1937}],
               'docCount': 42,
               'mentions': [{'docCount': 12, 'name': 'Musik'},
                            {'docCount': 10, 'name': 'Kantate'}],
               'topAuthors': [{'doc_count': 12, 'key': 'Karl'},
                              {'doc_count': 10, 'key': 'Orff'}]
               }
        assert response.status_code == 200

        data2 = deepcopy(data1)
        data1["docCount"] = 10001

        assert response.json == {"Topic1": {"linkedAgg": data1},
                                 "Topic2": {"linkedAgg": data2}}
    # GET
    response = client.get("/explore/aggregations?topics=Topic1&topics=Topic2")
    check_this(response)

    # POST
    query1 = json.dumps(topic_aggs_query_strict("Topic1"))
    query2 = json.dumps(topic_aggs_query_strict("Topic2"))

    response = client.post("/explore/aggregations",
                           json={"queries": [query1, query2],
                                 "topics": ["Topic1", "Topic2"]
                                 }
                           )
    check_this(response)

