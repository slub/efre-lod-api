import json
import flask
from flask_restx import Namespace
from flask_restx import reqparse
import elasticsearch


from lod_api.tools.resource import LodResource
from lod_api.tools.helper import ES_wrapper
from lod_api import CONFIG

from .explore_schema import (
        topicsearch_schema,
        aggregations_schema
        )

from .explore_queries import (
        topic_query,
        topic_aggs_query_strict,
        topic_aggs_query_loose
        )

api = Namespace(name="explorative search", path="/",
                description="API endpoint to be use with the explorative search webapp, see <URL>")


def translateBackendToWebapp():
    pass


class EntityMap:
    def __init__(self):
        pass

    @staticmethod
    def es2topics(doc):
        topic = {
            "id":            doc["@id"],
            "name":          doc["preferredName"],
            "alternateName": doc.get("alternateName", []),
            "description":   doc.get("description", ""),
            "additionalTypes": [],                # fill later on
        }
        if doc.get("additionalType"):
            # process all additionalType entries individually
            for i, adtype in enumerate(doc["additionalType"]):
                topic["additionalTypes"].append({
                    "id":          adtype.get("@id"),
                    "name":        adtype["name"],
                    "description": adtype["description"]
                    }
                )
                # remove none-existing id
                if not topic["additionalTypes"][i]["id"]:
                    del topic["additionalTypes"][i]["id"]
        return topic

    @staticmethod
    def es2persons(doc):
        person = {
            "id": doc["@id"],
            "name": doc["preferredName"],
            "birthPlace": doc.get("birthPlace"),
            "deathPlace": doc.get("deathPlace"),
            }
        if doc.get("birthDate") and doc["birthDate"].get("@value"):
            person["birthDate"] = doc["birthDate"]["@value"]
        if doc.get("deathDate") and doc["deathDate"].get("@value"):
            person["deathDate"] = doc["deathDate"]["@value"]
        return person

    @staticmethod
    def es2geo(doc):
        geo = {
            "id": doc["@id"],
            "name": doc["preferredName"]
            }
        if doc.get("geo"):
            geo["geo"] = doc["geo"]
        return geo

    @staticmethod
    def es2resources(doc):
        resource = {
            "id": doc["@id"],
            "name": doc["preferredName"]
            }
        return resource


def topicsearch_simple(es, query, excludes):
    """
    use POST query to make an elasticsearch search
    use `es`instance with query body `query` and
    exclude the fields given by `excludes`
    """
    retdata = []
    res = ES_wrapper.call(
            es,
            action="search",
            index="topics-explorativ",
            body=query,
            _source_excludes=excludes
        )

    if res["hits"] and res["hits"]["hits"]:
        for r in  res["hits"]["hits"]:
            elem = EntityMap.es2topics(r["_source"])
            elem["score"] = r["_score"]
            retdata.append(topicsearch_schema.validate(elem))
    return retdata

def aggregate_topics(es, topics, queries=None,
        aggs_fn=topic_aggs_query_strict,
        count_max=10000):
    """
    aggregate information concerning given `topics` using the
    elasticsearch query generated mit `aggs_fn`

    :param elasticserach es: elasticsearch instance
    :param list(str) topics: list of topic names
    :param fct aggs_fs: function to generate aggregatio
                        query for elasticsearch query
                        (takes topic name as argument)
    :param int count_max: maximal reportet hits by elasticsearch
                          instance. Used to trigger a second
                          scroll query to get the exact document
                          count.
    :return: dict with aggregations in the form
             {topics[i]: aggregations[i]}
    """
    if not queries:
        # generate query from topics
        # for multisearch
        queries = []
        for topic in topics:
            queries.append(
                    json.dumps(
                        aggs_fn(topic)
                        )
                    )
    query = '{}\n' + '\n{}\n'.join(queries)

    res = ES_wrapper.call(
            es,
            action="msearch",
            index="resources-explorativ",
            body=query,
        )
    aggregations = {}
    for i, r in enumerate(res["responses"]):
        agg_topAuthors = []
        agg_mentions = []
        agg_datePublished = []
        agg_genres = []

        agg = {}
        agg_docCount = r["hits"]["total"]["value"]
        if agg_docCount == count_max:
            # redo request via scroll request to get exact
            # hit count
            # TODO: simplify request (i.e. without aggs)
            r2 = ES_wrapper.call(
                    es,
                    action="search",
                    index="resources-explorativ",
                    scroll="1s",
                    body = json.loads(queries[i])
                )
            agg_docCount = r2["hits"]["total"]["value"]
        agg["docCount"] = agg_docCount

        # rename doc_count→docCount
        for bucket in r["aggregations"]["topAuthors"]["buckets"]:
            agg_topAuthors.append({
                "key": bucket["key"],
                "docCount": bucket["doc_count"]
                })

        # rename doc_count→docCount
        for bucket in r["aggregations"]["mentions"]["buckets"]:
            agg_mentions.append({
                "key": bucket["key"],
                "docCount": bucket["doc_count"]
                })

        # rename key_as_string→year, doc_count→count
        for bucket in r["aggregations"]["datePublished"]["buckets"]:
            agg_datePublished.append({
                "year": int(bucket["key_as_string"].split("-")[0]),
                "count": bucket["doc_count"]
                })

        agg["topAuthors"] = agg_topAuthors
        agg["mentions"] = agg_mentions
        agg["datePublished"] = agg_datePublished
        # TODO: validate
        if aggs_fn == topic_aggs_query_strict:
            aggregations[topics[i]] = \
                    aggregations_schema.validate(agg)
        elif aggs_fn == topic_aggs_query_loose:
            aggregations[topics[i]] = \
                    aggregations_schema.validate(agg)

    return aggregations


def merge_histogram(hist1, hist2):
    pass

def evaluate_entities(es, uris):
    def parse_index_id(uri):
        """
        split index and id from URI
        e.g. https://data.slub-dresden.de/topic/123456 → (topics, 123456)
        """
        return  uri.split("/")[-2], uri.split("/")[-1]
    entity_uris = {}
    entity_pool = {}

    for index, _id in map(parse_index_id, uris):
        try:
            entity_uris[index].append(_id)
        except KeyError:
            entity_uris[index] = []
            entity_uris[index].append(_id)

    # TODO: parallelize if too slow
    for entity, ids in entity_uris.items():
        res = ES_wrapper.call(
            es,
            action="mget",
            index=f"{entity}-explorativ",
            body={"ids": ids}
        )
        # collect and transform docs
        docs = {}
        for r in res["docs"]:
            docs[r["_source"]["@id"]] = \
                getattr(EntityMap, f"es2{entity}")(r["_source"])
        entity_pool[entity] = docs
    return entity_pool

def eval_aggs(es, topics, queries={"strict": None, "loose": None}):
    aggs_strict = aggregate_topics(es, topics,
                   aggs_fn=topic_aggs_query_strict,
                   queries=queries["strict"])
    aggs_loose = aggregate_topics(es, topics,
                   aggs_fn=topic_aggs_query_loose,
                   queries=queries["loose"])

    uris = set()
    for strategy in (aggs_strict, aggs_loose):
        for topic in topics:
            for agg in ("topAuthors", "mentions"):
                uris |= set([x["key"] for x in strategy[topic][agg]])

    # evaluate all entities found in every aggregation
    entity_pool = evaluate_entities(es, uris)

    unified_aggs = {
            "entityPool": entity_pool,
            "superAgg": {}
            }
    # add specific aggreatations based on topic name
    for topic in topics:
        unified_aggs[topic] = {
                "strictAgg": aggs_strict[topic],
                "looseAgg": aggs_loose[topic],
                }

    return unified_aggs

@api.route('/explore/topicsearch', methods=['GET', 'POST'])
class exploreTopics(LodResource):
    parser = reqparse.RequestParser()
    parser.add_argument('q', type=str, required=True,
            help="query string to search", location="args")
    parser.add_argument('size', type=int, default=15,
            help="size of the response", location="args")
    # available field to query against
    avail_qfields = [
            'preferredName',
            'alternateName',
            'description',
            'additionalType.description',
            'additionalType.name'
            ]
    parser.add_argument('fields', type=str, action="append",
            default=avail_qfields, choices=tuple(avail_qfields),
            help="list of internal elasticsearch fields to query against.",
            location="args")
    parser_post = reqparse.RequestParser()
    parser_post.add_argument('body', type=dict, required=True,
            help="query body object to be given through to elasticsearch",
            location="json")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('query topics')
    def get(self):
        """
        perform a simple serach on the topics index
        """
        print(type(self).__name__)

        es_host, es_port, excludes = CONFIG.get("es_host", "es_port", "excludes")
        es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

        args = self.parser.parse_args()
        query = topic_query(args.get("q"), args.get("size"), args.get("fields"), excludes)

        retdata = topicsearch_simple(es, query, excludes)
        return self.response.parse(retdata, "json", "", flask.request)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser_post)
    @api.doc('query topics with elasticsearch query')
    def post(self):
        """
        query topics with elasticsearch query object as {"body": query}
        """
        print(type(self).__name__)

        es_host, es_port, excludes = CONFIG.get("es_host", "es_port", "excludes")
        es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

        args = self.parser_post.parse_args()

        retdata = topicsearch_simple(es, args["body"], excludes)
        return self.response.parse(retdata, "json", "", flask.request)

@api.route('/explore/aggregations', methods=['GET', 'POST'])
class aggregateTopics(LodResource):
    parser = reqparse.RequestParser()
    # parser.add_argument('size', type=int, default=15,
    #         help="size of the response", location="args")
    # available field to query against
    parser.add_argument('topics', type=str, action="append", required=True,
            help="multiple topics to aggregate",
            location="args")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('aggregate topAuthors, datePublished and relatedTopics around topics')
    def get(self):
        """
        aggregate topAuthors, datePublished and relatedTopics around topics
        """
        print(type(self).__name__)

        es_host, es_port = CONFIG.get("es_host", "es_port")
        es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

        args = self.parser.parse_args()

        retdata = eval_aggs(es, args.get("topics"))
        return self.response.parse(retdata, "json", "", flask.request)

    parser_post = reqparse.RequestParser()
    parser_post.add_argument('queries', type=dict, required=True,
            help="aggregate body object (\\n-delimited) to be given through to elasticsearch",
            location="json")
    parser_post.add_argument('topics', type=list, required=True,
            help="list of topics name the aggreate queries are about",
            location="json")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser_post)
    @api.doc('aggregate topics via elasticsearch multiquery')
    def post(self):
        """
        aggregate topics via elasticsearch multiquery
        """
        print(type(self).__name__)

        es_host, es_port = CONFIG.get("es_host", "es_port")
        es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

        args = self.parser_post.parse_args()

        retdata = eval_aggs(es, args["topics"], queries=args["queries"])
        return self.response.parse(retdata, "json", "", flask.request)
