import json
import flask
from glom import (
        glom,
        core,
        Coalesce
        )
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


class EntityMapper:
    def __init__(self):
        pass

    @staticmethod
    def _clean_nones(data):
        # TODO: generalize cleaning of None-valued entries
        pass

    @staticmethod
    def es2topics(doc):
        spec = {
            'id': '@id',
            'name': 'preferredName',
            'alternateName': Coalesce('alternateName', default=[]),
            'description': Coalesce('description', default=""),
            'additionalTypes': (Coalesce('additionalType', default=[]), [{
                    'id': Coalesce('@id', default=None),
                    'name': 'name',
                    'description': 'description'
                    }]
                )
            }
        topic = glom(doc, spec)
        for adtype in topic["additionalTypes"]:
            if not adtype["id"]:
                del adtype["id"]

        return topic

    @staticmethod
    def es2persons(doc):
        spec = {
            'id': '@id',
            'name': 'preferredName',
            'alternateNames': Coalesce('alternateName', default=[]),
            'honorificSuffic': Coalesce('honorificSuffic.name', default=""),
            'birthPlace': Coalesce('birthPlace', default=None),
            'birthDate': Coalesce('birthDate.@value', default=None),
            'deathPlace': Coalesce('deathPlace', default=None),
            'deathDate': Coalesce('deathDate.@value', default=None),
            }
        person = glom(doc, spec)
        return person

    @staticmethod
    def es2geo(doc):
        spec = {
            'id': '@id',
            'name': 'preferredName',
            }
        geo = glom(doc, spec)
        return geo

    @staticmethod
    def es2organizations(doc):
        spec = {
            'id': '@id',
            'name': 'preferredName',
            }
        organizations = glom(doc, spec)
        return organizations

    @staticmethod
    def es2works(doc):
        spec = {
            'id': '@id',
            'name': 'preferredName',
            }
        works = glom(doc, spec)
        return works

    @staticmethod
    def es2resources(doc):
        # TODO: fix datePublished → wrong mapping?
        spec = {
            'id': '@id',
            'title': 'preferredName',
            'authors': Coalesce(
                            ('author', ['name']),
                            ('contributor', ['name']),
                            default=["TO_BE_MAPPED"]
                        ),
            'datePublished': Coalesce(
                            'datePublished.0.0.@value',
                            'datePublished.0.@value',
                            'datePublished.@value',
                            default=None
                        ),
            'inLanguage': Coalesce('inLanguage', default=None),
            'description': Coalesce('description', default=""),
            }
        resource = glom(doc, spec)
        if resource.get("datePublished"):
            resource["yearPublished"] = resource["datePublished"].split("-")[0]
        return resource

class AggregationManager():
    """
    generate output based on aggregation of the form:
       result = {
         "aggMethod1": {
           "subjects": {
             "aggSubject1": {
                "aggs": {"specificAgg1": [], "specificAgg2: []},
                "docCount": 42,
                "resources": []
                 },
              "aggSubject2": {}
                   },
           "superAgg": {}
         },
         "aggMethod2": {
           "aggs": {"aggSubject1": {}, "aggSubject2": {}},
           "superAgg": {}
         },
         "entityPool": {}
       }

    """
    def __init__(self, es, aggregations):
        self.es = es
        self.result = {
                "entityPool": {
                    "resources": {}
                    }
                }
        self.agg_names = []      # names of specific aggregations
                                 #  defined within the es query
        self.agg_subjects = []   # list of aggregation subjects

        self.agg_methods = []    # name of aggregation methods
        self.agg_query_fcts = [] # function that generates es
                                 #  query dicts for aggregation

        self.entity_pool = {}

        self.register_agg_methods(aggregations)

    def register_agg_methods(self, aggregations):
        """
        :param dict aggregations {"aggMethod1": query_function1, …}
        """
        for method in aggregations:
            self.result[method] = {}
            self.result[method]["subjects"] = {}
        self.agg_methods = list(aggregations.keys())
        self.agg_query_fcts = list(aggregations.values())

    def add_agg_subjects(self, subj):
        """
        :param list subj
        """
        self.agg_subjects = subj
        pass


    def _parse_agg(self, agg):
        """
        parse aggregation into a more pythonic form - from
        elasticsearch {key: name, value: count} to {name: count}
        - if key-names are present multiple times: add their value-count
        - prefere "key_as_string" before "key"
        """
        if agg == []:
            return {}
        if "key_as_string" in agg[0]:
            key = "key_as_string"
        else:
            key = "key"
        agg_list = []
        for agg_elem in agg:
            if key == "key_as_string":
                # transform date into year by splitting with "-"
                agg_key = agg_elem[key].split("-")[0]
            else:
                agg_key = agg_elem[key]
            agg_list.append({agg_key: agg_elem["doc_count"]})
        return self._add_aggs(agg_list)

    def run_aggs(self, queries=None, es_limit=10000):
        """
        construct es mulitQuery and run
        """
        if not queries:
            # generate query from topics for multisearch
            # ATTENTION: the queries order is important here, as
            # we have to remember it for later to correctly
            # synchronize the responses with the methods/subjects.

            queries = []
            for query_fct in self.agg_query_fcts:
                for i, subj in enumerate(self.agg_subjects):
                    queries.append(
                            json.dumps(
                                query_fct(subj)
                                )
                            )
        else:
            # TODO we have to ensure the order also here
            queries = [json.dumps(q) for q in queries]

        query = '{}\n' + '\n{}\n'.join(queries)

        res = ES_wrapper.call(
                self.es,
                action="msearch",
                index="resources-explorativ",
                body=query,
            )
        # iterate over responses respective to queries, i.e.:
        #     [method1subj1, method1subj2, method2subj1, method2subj2]
        # reverse list and pop each element to use the same iteration again
        ctr = 0                            # counter corresponding to queries list
        for method in self.agg_methods:
            self.result[method]["subjects"] = {}
            for subj in self.agg_subjects:
                self.result[method]["subjects"][subj] = {"aggs": {}}
                # extract aggregation by popping item
                r = res["responses"][ctr]
                # store docCount for found resources
                # TODO: rerun queries that are limited by elasticsearch (10000)
                doc_count = r["hits"]["total"]["value"]
                if doc_count == es_limit:
                    res2 = ES_wrapper.call(
                            self.es,
                            action="search",
                            index="resources-explorativ",
                            scroll="1s",
                            body = json.loads(queries[ctr])
                        )
                    doc_count = res2["hits"]["total"]["value"]
                self.result[method]["subjects"][subj]["docCount"] = doc_count
                # iterate over aggs defines in es-query
                for agg in r["aggregations"]:
                    if agg not in self.agg_names:
                        self.agg_names.append(agg)
                    self.result[method]["subjects"][subj]["aggs"][agg] = \
                        self._parse_agg(r["aggregations"][agg]["buckets"])

                # extract resources
                self.result[method]["subjects"][subj]["resources"] = []
                for hit in r["hits"]["hits"]:
                    _id = hit["_source"]["@id"]
                    # sort score into aggregation …
                    self.result[method]["subjects"][subj]["resources"].append({
                                  "id": _id,
                                  "score": hit["_score"]
                                }
                            )
                    # … and document into entityPool
                    self.result["entityPool"]["resources"][_id] = \
                            EntityMapper.es2resources(hit["_source"])
                ctr += 1
        self._merge_agg_subjects()

    def _add_aggs(self, aggs):
        """
        merge a list of aggregations (i.e. dicts) by adding their values
        if keys coincide
        """
        result = {}
        for agg in aggs:
            for k, v in agg.items():
                if result.get(k):
                    result[k] += v
                else:
                    result[k] = v
        return result

    def _merge_agg_subjects(self):
        """
        Collect aggregation results from all aggregations with `agg_name`
        results and sum them up.
        Aggregations are assumed to be in a somehow of form {name: count}
        but stored in the elasticsearch specific {key: name, value: count}
        format.
        """
        # TODO: do before translating aggregations for the webapp
        for method in self.agg_methods:
            self.result[method]["superAgg"] = {}
            for agg in self.agg_names:
                aggs = []
                for subj in self.agg_subjects:
                    aggs.append(self.result[method]["subjects"][subj]["aggs"][agg])
                self.result[method]["superAgg"][agg] = self._add_aggs(aggs)

    def resolve_agg_entities(self, prefix="https://data.slub-dresden.de"):
        """
        gather all IDs that are given back by aggregations and collect them
        if they start with a given prefix (here, our api-base-URI)
        """
        uris = set()
        for method in self.agg_methods:
            for subj in self.agg_subjects:
                for agg_name in self.agg_names:
                    agg = self.result[method]["subjects"][subj]["aggs"][agg_name]
                    uris |= set([x for x in agg if x.startswith(prefix)])
        self.query_entities_by_uri(pool_uris=uris)

    def query_entities_by_uri(self, pool_uris=[]):
        """
        collect all entity-objects that are given by their URIs
        """
        def parse_index_id(uri):
            """
            split index and id from URI
            e.g. https://data.slub-dresden.de/topic/123456 → (topics, 123456)
            """
            return  uri.split("/")[-2], uri.split("/")[-1]

        entity_uris = {}
        for index, _id in map(parse_index_id, pool_uris):
            try:
                entity_uris[index].append(_id)
            except KeyError:
                entity_uris[index] = []
                entity_uris[index].append(_id)

        # TODO: parallelize if too slow
        for entity, ids in entity_uris.items():
            # TODO: remove and take from config
            if entity == "swb":
                continue

            res = ES_wrapper.call(
                self.es,
                action="mget",
                index=f"{entity}-explorativ",
                body={"ids": ids}
            )
            # collect and transform docs
            self.result["entityPool"][entity] = {}
            for r in res["docs"]:
                _id = r["_source"]["@id"]
                self.result["entityPool"][entity][_id] = \
                    getattr(EntityMapper, f"es2{entity}")(r["_source"])


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
            elem = EntityMapper.es2topics(r["_source"])
            elem["score"] = r["_score"]
            retdata.append(topicsearch_schema.validate(elem))
    return retdata

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

        am = AggregationManager(es, aggregations={
            "topicMatch": topic_aggs_query_strict,
            "phraseMatch": topic_aggs_query_loose
            })
        am.add_agg_subjects(args.get("topics"))
        am.run_aggs()
        am.resolve_agg_entities()
        return self.response.parse(am.result, "json", "", flask.request)

    parser_post = reqparse.RequestParser()
    parser_post.add_argument('queries', type=dict, required=True,
            help="list of different query objects to be given through to elasticsearch",
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

        am = AggregationManager(es, aggregations={
            "topicMatch": topic_aggs_query_strict,
            "phraseMatch": topic_aggs_query_loose
            })
        am.add_agg_subjects(args.get("topics"))
        am.run_aggs(queries=(args["queries"]["topicMatch"] + args["queries"]["phraseMatch"]))
        am.resolve_agg_entities()
        return self.response.parse(am.result, "json", "", flask.request)
