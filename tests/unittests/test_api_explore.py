import pytest
from copy import deepcopy
from lod_api.apis.explore import *
from lod_api.apis.explore_queries import (
        topic_query,
        topic_aggs_query_phraseMatch,
        topic_aggs_query_topicMatch,
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
                        },
                    "hits":[{
                        "_score": 1,
                        "_source": {
                            "@id": 12345678,
                            "preferredName": "resource queried in aggregation"
                            }
                        }]
                    },
                "aggregations": {
                    "topAuthors": {
                        "buckets": [
                            {"key": "/persons/Karl", "doc_count": 12},
                            {"key": "/persons/Orff", "doc_count": 10}
                            ]
                        },
                    "mentions": {
                        "buckets": [
                            {"key": "/resources/Musik", "doc_count": 12},
                            {"key": "/topics/Kantate", "doc_count": 10}
                            ]
                        },
                    "datePublished": {
                        "buckets": [
                            {"key_as_string": "1937-06-08", "doc_count": 12},
                            ]
                        },
                    }
                }
        self.aggregate_topics_matrix_resp = {
                "hits": {
                    "total": {
                        "value": 50
                        },
                    "hits":[]
                    },
                "aggregations": {
                    "topicAM": {
                        "buckets": [
                            {"key": "Topic1&Topic2", "doc_count": 12},
                            {"key": "Topic1", "doc_count": 15},
                            {"key": "Topic2", "doc_count": 16}
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
        elif (kwargs["body"].get("aggs") and
                kwargs["body"]["aggs"].get("topAuthors") and
                kwargs["body"]["aggs"].get("mentions") and
                kwargs["body"]["aggs"].get("datePublished")):
            # topics aggregate strict detected (used with scroll
            # api to get correct hits value)
            resp = deepcopy(self.aggregate_topics_strict_resp)
            resp["hits"]["total"]["value"] = 10001
            return resp
        elif (kwargs["body"].get("aggs") and
                kwargs["body"]["aggs"].get("topicAM")):
            resp = deepcopy(self.aggregate_topics_matrix_resp)
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

    def mget(self, *args, **kwargs):
        if kwargs["index"] == "resources-explorativ":
            return {"docs": [
                    {"_source": {"@id": "1234567", "preferredName": "one document",
                        "author": "Hans",
                        "datePublished": {"@value": "2021-04-25"},
                        "inLanguage": ["de"],
                        "description": "some text",
                        }}
                    ]
                }
        else:
            return {"docs": [
                    {"_source": {"@id": "1234567", "preferredName": "one document"}}
                    ]
                }





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
    def check_this(response, center=False):
        """ Check response from /aggregations endpoint """

        agg1 = {
            'aggs': {
                'datePublished': {'1937': 12},
                'mentions': {'/resources/Musik': 12,
                             '/topics/Kantate': 10},
                'topAuthors': {'/persons/Karl': 12,
                               '/persons/Orff': 10}
                },
            "resources": [{'id': 12345678, 'score': 1}],
            'docCount': 42,
            }
        resources = {12345678: {'authors': ['TO_BE_MAPPED'],
                                'datePublished': None,
                                'description': '',
                                'id': 12345678,
                                'inLanguage': None,
                                'title': 'resource queried in aggregation'}}
        assert response.status_code == 200

        agg2 = deepcopy(agg1)
        if center:
            agg1["correlations"] = {
                    "topicAM": {
                        "Topic1": 15,
                        "Topic1&Topic2": 12,
                        "Topic2": 16
                        }
                    }

        assert response.json["phraseMatch"]["subjects"] == {
                    "Topic1": agg1,
                    "Topic2": agg2
                    }

        agg1["docCount"] = 10001
        assert response.json["topicMatch"]["subjects"] == {
                    "Topic1": agg1,
                    "Topic2": agg2
                    }
    # GET
    response = client.get("/explore/aggregations?topics=Topic1&topics=Topic2")
    check_this(response)

    # simply check if restrict is also accepted
    response = client.get("/explore/aggregations?topics=Topic1&topics=Topic2&restricts=Topic3")
    check_this(response)

    # simply check if center is also accepted
    response = client.get("/explore/aggregations?topics=Topic1&topics=Topic2&center=Topic1")
    check_this(response, center=True)

    # POST
    template_topic = topic_aggs_query_topicMatch("$subject")
    template_phrase = topic_aggs_query_phraseMatch("$subject")

    response = client.post("/explore/aggregations",
            json={"queryTemplate": {
                        "topicMatch": template_topic,
                        "phraseMatch": template_phrase
                    },
                    "topics": ["Topic1", "Topic2"]
                 }
             )
    check_this(response)

