import flask
from flask_restplus import Namespace
from flask_restplus import reqparse
from elasticsearch import Elasticsearch

from lod_api import CONFIG
from lod_api.tools.resource import LodResource

api = Namespace(name="authority_search", path="/",
                description="Authority Provider Identifier Search")


# flaskREST+ BUG, which ignores the last element in <any([…])> list
#     [see](https://github.com/noirbizarre/flask-restplus/issues/695)
# quickfix: add whitespace string as element
@api.route('/<any({}):authority_provider>/<string:id>'
           .format(CONFIG.get("authorities_list") + [" "]),
           methods=['GET']
           )
@api.param('authority_provider',
           'The name of the authority-provider to access. '
           'Allowed Values: {}.'.format(CONFIG.get("authorities_list")))
@api.param('id', 'The ID-String of the authority-identifier to access. '
           'Possible Values (examples): 208922695, 118695940, 20474817, Q1585819')
class AutSearch(LodResource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        'format', type=str, help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json", location="args")
    parser.add_argument(
        'size', type=int, help="Configure the maxmimum amount of hits to be returned", location="args", default=100)
    parser.add_argument(
        'from', type=int, help="Configure the offset from the frist result you want to fetch", location="args", default=0)
    es_host, es_port, excludes, indices, authorities = CONFIG.get("es_host", "es_port", "excludes", "indices", "authorities")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('get record by authority-id')
    def get(self, authority_provider, id):
        """
        search for an given ID of a given authority-provider
        """
        print(type(self).__name__)
        retarray = []
        args = self.parser.parse_args()
        name = ""
        ending = ""
        if "." in id:
            dot_fields = id.split(".")
            name = dot_fields[0]
            ending = dot_fields[1]
        else:
            name = id
            ending = ""
        if authority_provider not in self.authorities:
            flask.abort(404)
        search = {"_source": {"excludes": self.excludes}, "query": {"query_string": {
            "query": "sameAs.keyword:\"" + self.authorities.get(authority_provider) + name + "\""}}}
        res = self.es.search(index=','.join(CONFIG.get("indices_list")), body=search, size=args.get("size"), from_=args.get("from"), _source_exclude=self.excludes)
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return self.response.parse(retarray, args.get("format"), ending, flask.request)


# flaskREST+ BUG, which ignores the last element in <any([…])> list
#     [see](https://github.com/noirbizarre/flask-restplus/issues/695)
# quickfix: add whitespace string as element
@api.route('/<any({aut}):authority_provider>/<any({ent}):entity_type>'
           '/<string:id>'.format(aut=CONFIG.get("authorities_list") + [" "],
                                 ent=CONFIG.get("indices_list") + [" "]),
           methods=['GET'])
@api.param('authority_provider',
           'The name of the authority-provider to access. '
           'Allowed Values: {}.'.format(CONFIG.get("authorities_list")))
@api.param('entity_type', 'The name of the entity-index to access. '
           'Allowed Values: {}.'.format(CONFIG.get("indices_list")))
@api.param('id', 'The ID-String of the authority-identifier to access. '
           'Possible Values (examples): 208922695, 118695940, 20474817, Q1585819')
class AutEntSearch(LodResource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        'format', type=str, help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json", location="args")
    parser.add_argument(
        'size', type=int, help="Configure the maxmimum amount of hits to be returned", location="args", default=100)
    parser.add_argument(
        'from', type=int, help="Configure the offset from the frist result you want to fetch", location="args", default=0)
    es_host, es_port, excludes, indices, authorities = CONFIG.get("es_host", "es_port", "excludes", "indices", "authorities")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('get record by authority-id and entity-id')
    def get(self, authority_provider, entity_type, id):
        """
        search for an given ID of a given authority-provider on a given entity-index
        """
        print(type(self).__name__)
        retarray = []
        args = self.parser.parse_args()
        name = ""
        ending = ""
        if "." in id:
            dot_fields = id.split(".")
            name = dot_fields[0]
            ending = dot_fields[1]
        else:
            name = id
            ending = ""
        if authority_provider not in self.authorities or entity_type not in CONFIG.get("indices_list"):
            flask.abort(404)
        search = {"_source": {"excludes": self.excludes},
                  "query": {"query_string": {"query": "sameAs.keyword:\"" + self.authorities.get(authority_provider) + name + "\""}}
                  }
        res = self.es.search(index=entity_type, body=search, size=args.get(
            "size"), from_=args.get("from"), _source_exclude=self.excludes)
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return self.response.parse(retarray, args.get("format"), ending, flask.request)
