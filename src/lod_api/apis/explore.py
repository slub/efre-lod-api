import json
import flask
from flask_restx import Namespace
from flask_restx import reqparse
import elasticsearch


from lod_api.tools.resource import LodResource
from lod_api.tools.helper import ES_wrapper
from lod_api import CONFIG

from .explore_schema import topicsearch
from .explore_queries import topic_aggs_query_strict

api = Namespace(name="explorative search", path="/",
                description="API endpoint to be use with the explorative search webapp, see <URL>")


def translateBackendToWebapp():
    pass

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
            elem = {
                "id":            r["_source"]["@id"],
                "score":         r["_score"],
                "name":          r["_source"]["preferredName"],
                "alternateName": r["_source"].get("alternateName", []),
                "description":   r["_source"].get("description", ""),
                "additionalTypes": [],                # fill later on
            }
            if r["_source"].get("additionalType"):
                # process all additionalType entries individually
                for i, adtype in enumerate(r["_source"]["additionalType"]):
                    elem["additionalTypes"].append({
                        "id":          adtype.get("@id"),
                        "name":        adtype["name"],
                        "description": adtype["description"]
                        }
                    )
                    # remove none-existing id
                    if not elem["additionalTypes"][i]["id"]:
                        del elem["additionalTypes"][i]["id"]
            retdata.append(topicsearch.validate(elem))
    return retdata

def aggregate_topics(es, topics, excludes,
        fields=['preferredName^2',
                'description',
                'alternativeHeadline',
                'nameShort',
                'nameSub',
                'author.name',
                'mentions.name^3',
                'partOfSeries.name',
                'about.name',
                'about.keywords']
        ):

    # aggregate queries for multisearch
    queries = []
    for hit in topics:
        queries.append(
                json.dumps(
                    topic_aggs_query_strict(hit["name"], fields)
                    )
                )

    query = '{}\n' + '\n{}\n'.join(queries)

    res = ES_wrapper.call(
            es,
            action="msearch",
            index="resources-explorativ",
            body=query,
        )

    for i, r in enumerate(res["responses"]):
        agg_topAuthors = []
        agg_mentions = []
        agg_datePublished = []
        agg_genres = []

        agg = {}
        agg["docCount"] = r["hits"]["total"]["value"]
        agg_topAuthors = r["aggregations"]["topAuthors"]["buckets"]

        # rename key→name, doc_count→docCount
        for bucket in r["aggregations"]["mentions"]["buckets"]:
            agg_mentions.append({
                "name": bucket["key"],
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
        topics[i]["aggregations"] = agg
        topics[i] = topicsearch.validate(topics[i])

    return topics

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
        # TOOD: multi_match types best_fields, most_fields, cross_fields, phrase, phrase_prefix, bool_prefix
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
        # multi_match:
        #   query,
        #   fields: ['*'],
        #
        # type: 'most_fields'
        # }
        topicquery = {
                'size': args.get("size"),
                '_source': excludes,
                'query': {
                    "simple_query_string": {
                        'query':  args.get("q"),
                        'fields': args.get("fields"),
                        'default_operator': 'and'
                        }
                    }
                }

        topicsearch = topicsearch_simple(es, topicquery, excludes)
        retdata = aggregate_topics(es, topicsearch, excludes)
        return self.response.parse(retdata, "json", "", flask.request)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser_post)
    @api.doc('query topics with elasticsearch query')
    def post(self):
        """
        perform a simple serach on the topics index
        """
        print(type(self).__name__)

        es_host, es_port, excludes = CONFIG.get("es_host", "es_port", "excludes")
        es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

        args = self.parser_post.parse_args()

        topicsearch = topicsearch_simple(es, args["body"], excludes)
        retdata = aggregate_topics(es, topicsearch, excludes)
        return self.response.parse(retdata, "json", "", flask.request)
