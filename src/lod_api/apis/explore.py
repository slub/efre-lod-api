import json
import flask
from glom import (
        SKIP,
        glom,
        core,
        Coalesce
        )
from flask_restx import (
        Namespace,
        reqparse,
        inputs
        )
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
        topic_resource_mentionCount,
        topic_aggs_query_topicMatch,
        topic_aggs_query_phraseMatch,
        topic_maggs_query_topicMatch,
        topic_maggs_query_phraseMatch
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
        # TODO: clean ??? in additionalTyple.name by cleaning data
        spec = {
            'id': '@id',
            'name': 'preferredName',
            'alternateName': Coalesce('alternateName', default=[]),
            'description': Coalesce('description', default=""),
            'additionalTypes': (Coalesce('additionalType', default=[]), [{
                    'id': Coalesce('@id', default=None),
                    'name': Coalesce('name', default="???"),
                    'description': Coalesce('description', default="")
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
        # FIXME, data-BUG: remove name.de.0 mapping in favor of preferredName
        spec = {
            'id': '@id',
            'name': Coalesce('preferredName', 'name.de.0', 'name.en.0', default="Unbekannt"),
            'alternateNames': Coalesce('alternateName', default=[]),
            'honorificSuffic': Coalesce('honorificSuffic.name', default=""),
            'birthPlace': Coalesce('birthPlace', default=None),
            'birthDate': Coalesce('birthDate.@value', default=None),
            'deathPlace': Coalesce('deathPlace', default=None),
            'deathDate': Coalesce('deathDate.@value', default=None),
            'occupation': (Coalesce('hasOccupation', default=[]), [Coalesce("name", default=SKIP)]),
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
    def es2events(doc):
        spec = {
            'id': '@id',
            'name': 'preferredName',
            }
        events = glom(doc, spec)
        return events

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
            'mentions': (Coalesce('mentions', default=[]), [{
                    "id": Coalesce('@id', default=None),
                    "name": Coalesce('name', default=None)
                }])
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
           "correlations": {"corrAgg": {}}.
           "superAgg": {}
         },
         "aggMethod2": {
           "aggs": {"aggSubject1": {}, "aggSubject2": {}},
           "correlations": {"corrAgg": {}}.
           "superAgg": {}
         },
         "entityPool": {}
       }

    """
    def __init__(self, es, aggregations):
        self.es = es
        # result for aggregations
        self.result = {
                "entityPool": {
                    "resources": {}
                    }
                }
        # result for correlations
        self.correlations = {}

        self.agg_names = []      # names of specific aggregations
                                 #  defined within the es query
        self.agg_subjects = []   # list of aggregation subjects

        self.agg_methods = []    # name of aggregation methods
        self.agg_query_fcts = [] # function that generates es
                                 #  query dicts for aggregation
        self.magg_query_fcts = [] # function that generates query
                                  #  for matrix aggregation

        self.entity_pool = {}

        self.register_agg_methods(aggregations)

    def register_agg_methods(self, aggregations):
        """
        :param dict aggregations {"aggMethod1": query_function1, …}
        OR with two functions for aggreation + matrix aggregation as tuple:
        :param dict aggregations {"aggMethod1": (query_function1, maxtrix_function), …}
        """
        for method in aggregations:
            self.result[method] = {}
            self.result[method]["subjects"] = {}
            self.agg_methods.append(method)
            if (isinstance(aggregations[method], tuple) and
                    len(aggregations[method]) == 2):
                self.agg_query_fcts.append(aggregations[method][0])
                self.magg_query_fcts.append(aggregations[method][1])
            else:
                self.agg_query_fcts.append(aggregations[method])

    def add_agg_subjects(self, subj, filter1=None):
        """
        :param list subj
        :param string filter1 Filter to apply to all queries
        """
        self.agg_subjects = subj
        self.agg_filter1 = filter1      # author filter
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

    def run_aggs(self, query_template=None, restriction=None, es_limit=10000):
        """
        construct elasticsearch mulitQuery for aggregations, run and evaluate

        :param dict query_template - is a combination of templates assigned to
        each aggregation method. If query templates are given, all entities of
        "$subject" are replaced with a correspondic subject during creation of
        the elasticsearch query objects. If no query_template is given, the
        registered functions self.agg_query_fcts are used to generate query dicts
        based on the subjects.
        e.g.
              query_template = {
                    "topicMatch": {"toBeReplaced": "$subject"},
                    "phraseMatch": {…}
                  }

        :param str restriction - is treated as a second querystring which
        is given to the functions generating the elasticsearch query
        :param int es_limit - limit which is set on elasticsearch for simple
        queries. If a hit count with this exact number is returned, the query
        is run again to reveal the exact count of matching documents
        """
        queries = []
        if not query_template:
            # generate query from topics for multisearch
            # ATTENTION: the queries order is important here, as
            # we have to remember it for later to correctly
            # synchronize the responses with the methods/subjects.

            for query_fct in self.agg_query_fcts:
                for i, subj in enumerate(self.agg_subjects):
                    if restriction:
                        querystring = [subj, restriction]
                    else:
                        querystring = [subj]
                    queries.append(
                            json.dumps(
                                query_fct(querystring, filter1=self.agg_filter1)
                                )
                            )
        else:
            # replace template string $subject for every method and every
            # subject in the same order this would be done without the
            # template
            for method in self.agg_methods:
                for subj in self.agg_subjects:
                    # serialize and replace in string within json dump
                    # for each query method to given with query_template
                    queries.append(
                            json.dumps(
                                query_template[method]
                                ).replace("$subject", subj)
                            )

        query = '{}\n' + '\n{}\n'.join(queries)

        res = ES_wrapper.call(
                self.es,
                action="msearch",
                index="resources-explorativ",
                body=query,
            )
        # iterate over responses respective to queries, i.e.:
        #     [method1subj1, method1subj2, method2subj1, method2subj2]
        ctr = 0                            # counter corresponding to queries list
        for method in self.agg_methods:
            self.result[method]["subjects"] = {}
            for subj in self.agg_subjects:
                self.result[method]["subjects"][subj] = {"aggs": {}}
                # extract aggregations
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
                self.result[method]["subjects"][subj]["topResources"] = {}
                for hit in r["hits"]["hits"]:
                    _id = hit["_source"]["@id"]
                    # sort score into aggregation …
                    self.result[method]["subjects"][subj]["topResources"][_id] = \
                                  hit["_score"]
                    # … and document into entityPool
                    self.result["entityPool"]["resources"][_id] = \
                            EntityMapper.es2resources(hit["_source"])
                ctr += 1
        self._merge_agg_subjects()

    def run_correlations(self, subjects, query_templates=None):
        """
        correlate subjects via elasticsearch with different according to different
        aggregation methods
        """
        if query_templates:
            pass
        if len(self.magg_query_fcts) > 0:
            for i, query_matagg_fct in enumerate(self.magg_query_fcts):
                method = self.agg_methods[i]

                query = query_matagg_fct(subjects)
                matagg_res = ES_wrapper.call(
                        self.es,
                        action="search",
                        index="resources-explorativ",
                        body=query
                    )
                # gather matrix aggregation in correlations
                self.correlations[method] = {}
                for agg in matagg_res["aggregations"]:
                    self.correlations[method][agg] = \
                        self._parse_agg(matagg_res["aggregations"][agg]["buckets"])

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
                if not r["found"]:
                    continue
                _id = r["_source"]["@id"]
                self.result["entityPool"][entity][_id] = \
                    getattr(EntityMapper, f"es2{entity}")(r["_source"])


def topicsearch_simple(es, topic=None, size=None,
                       fields=None, query=None, excludes=None):
    """
    use POST query to make an elasticsearch search
    use `es`instance with query body `query` and
    exclude the fields given by `excludes`
    :param str topic - topic string to query for
    :param int size - count of valid results that should at most be returned
                      (i.e. mentionCount > 0). For the invalid (i.e. mentionCount == 0)
                      results, they are also returned according to this size.
                      However, the gathering of return data is ranked by elasticsearch
                      query score and is stopped as soon as `size`-count valid results
                      are found
    :param list(str) fields - list of elasticsearch fields that should be queried
    :param dict query - if the complete query is given all other parameters
                        are ignored (optional)
    """
    ret_data = []           # mapped data to be returned
    valid_result_size = 0   # count valid results (mentionCount > 0)
    invalid_result_size = 0 # count invalid results (mentionCount == 0)
    q_from = 0

    if query:
        # get size out of query to later on
        # decide whether we have enough results
        # (depending on mentionCount)
        size = query["size"]
        # extract topic for debugging purposes
        topic = query.get("query", {}).get("multi_match", {}).get("query")

    # query more than we need to have at least
    # `size` number of documents with mentionCount > 0
    # later on we can increase the q_from parameter to trigger
    # another search that continues
    extended_size = 3*size

    # iterate over multiple query chunks (by incrementing q_from)
    # in order to get `size` appropriate results with mentionCount > 0
    while True:
        qry_data = []           # data sets queried from elasticsearch
        doc_ids = []            # @ids of topics to query for resources
                                #  linked to this topic later on
        if all((topic, size, fields, excludes)) and not query:
            query = topic_query(topic, extended_size, fields,
                                excludes, q_from=q_from)
        elif query:
            query["from"] = q_from
            query["size"] = extended_size
        else:
            raise ValueError("as argument either `query` or "
                             "(`topic`, `size`, `fields`, and `excludes`) "
                             "is needed")

        res = ES_wrapper.call(
                es,
                action="search",
                index="topics-explorativ",
                body=query,
                _source_excludes=excludes
            )
        results_total = res["hits"]["total"]["value"]

        if res["hits"] and res["hits"]["hits"]:
            for r in  res["hits"]["hits"]:
                elem = EntityMapper.es2topics(r["_source"])
                elem["score"] = r["_score"]
                doc_ids.append(r["_source"]["@id"])
                qry_data.append(elem)
        else:
            # no hits, empty list, return nothing more
            return ret_data

        # take all ids and query how much resources are linked
        # to topics with this id. It is necessarry to use the id
        # here as we need this connection later on for the aggregations
        msearch_query = '{}\n' + '\n{}\n'.join([
                        json.dumps(topic_resource_mentionCount(_id))
                            for _id in doc_ids
                    ]
                )
        res_counts = ES_wrapper.call(
                es,
                action="msearch",
                index="resources-explorativ",
                body=msearch_query
            )

        # add mentionCount and validate schema
        for i, data in enumerate(qry_data):
            doc_count = res_counts["responses"][i]["hits"]["total"]["value"]
            data["mentionCount"] = doc_count

            if doc_count > 0:
                valid_result_size += 1
                if valid_result_size <= size:
                    ret_data.append(topicsearch_schema.validate(data))
                else:
                    break
            else:
                invalid_result_size += 1
                if invalid_result_size <= size:
                    ret_data.append(topicsearch_schema.validate(data))

        if valid_result_size >= size:
            # already got the wanted result size
            break

        if q_from + extended_size >= results_total:
            # no more results to query
            break
        else:
            if q_from == 0:
                print(f"explore: topic '{topic}' needed more than one query "
                      f"({extended_size}/{results_total})")
            q_from += extended_size

    return ret_data

@api.route('/explore/topicsearch', methods=['GET', 'POST'])
class exploreTopics(LodResource):
    parser = reqparse.RequestParser()
    parser.add_argument('q', type=str, required=True,
            help="query string to search", location="args")
    parser.add_argument('size', type=int, default=15,
            help="size of the response", location="args")
    # available field to query against
    avail_qfields = [
            'preferredName^2',
            'alternateName',
            'description',
            'additionalType.description',
            'additionalType.name'
            ]
    parser.add_argument('fields', type=str, action="append",
            default=avail_qfields,
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

        retdata = topicsearch_simple(es, topic=args.get("q"),
                                     size=args.get("size"),
                                     fields=args.get("fields"),
                                     excludes=excludes
                                    )
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

        retdata = topicsearch_simple(es, query=args["body"], excludes=excludes)
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
    parser.add_argument('author', type=str, required=False,
            help="use this specific author name as filter for the aggregation result",
            location="args")
    parser.add_argument('restrict', type=str, required=False,
            help="restrict all topic queries to occurrences with this restriction-topic",
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
            "topicMatch": (topic_aggs_query_topicMatch,
                           topic_maggs_query_topicMatch),
            "phraseMatch": (topic_aggs_query_phraseMatch,
                           topic_maggs_query_phraseMatch)
            })
        am.add_agg_subjects(args.get("topics"), filter1=args.get("author"))
        am.run_aggs(restriction=args.get("restrict"))
        am.resolve_agg_entities()
        return self.response.parse(am.result, "json", "", flask.request)

    parser_post = reqparse.RequestParser()
    parser_post.add_argument('queryTemplate', type=dict, required=True,
            help="template objects for each aggregation method, where the string "+
                 "\"$subject\" is replaced with each topic",
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
            "topicMatch": (topic_aggs_query_topicMatch,
                           topic_maggs_query_topicMatch),
            "phraseMatch": (topic_aggs_query_phraseMatch,
                           topic_maggs_query_phraseMatch)
            })
        am.add_agg_subjects(args.get("topics"))

        query_template = args["queryTemplate"]

        am.run_aggs(query_template=query_template)
        am.resolve_agg_entities()
        return self.response.parse(am.result, "json", "", flask.request)

@api.route('/explore/correlations', methods=['GET'])
class correlateTopics(LodResource):
    parser = reqparse.RequestParser()
    # parser.add_argument('size', type=int, default=15,
    #         help="size of the response", location="args")
    # available field to query against
    parser.add_argument('topics', type=str, action="append", required=True,
            help="multiple topics to correlate",
            location="args")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('correlate topics with their mutual occurances')
    def get(self):
        """
        correlate topics with their mutual occurances
        """
        print(type(self).__name__)

        es_host, es_port = CONFIG.get("es_host", "es_port")
        es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

        args = self.parser.parse_args()

        am = AggregationManager(es, aggregations={
            "topicMatch": (topic_aggs_query_topicMatch,
                           topic_maggs_query_topicMatch),
            "phraseMatch": (topic_aggs_query_phraseMatch,
                           topic_maggs_query_phraseMatch)
            })
        am.run_correlations(args.get("topics"))
        return self.response.parse(am.correlations, "json", "", flask.request)
