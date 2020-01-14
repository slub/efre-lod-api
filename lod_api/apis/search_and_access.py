import flask
from flask_restplus import Namespace
from flask_restplus import reqparse
import elasticsearch


from lod_api.resource import LodResource
from lod_api import CONFIG

api = Namespace(name="search and access", path="/",
                description="Search and Access Operations")


@api.route('/<any({ent}):entity_type>/search'.format(ent=CONFIG.get("indices_list") + [" "]), methods=['GET'])
@api.param('entity_type', 'The name of the entity-type to access. Allowed Values: {}.'.format(CONFIG.get("indices_list")))
class searchDoc(LodResource):
    parser = reqparse.RequestParser()
    parser.add_argument('q', type=str, help="Lucene Query String Search Parameter", location="args")
    parser.add_argument(
        'format', type=str, help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json", location="args")
    parser.add_argument(
        'size', type=int, help="Configure the maxmimum amount of hits to be returned", location="args", default=100)
    parser.add_argument(
        'from', type=int, help="Configure the offset from the frist result you want to fetch", location="args", default=0)
    parser.add_argument(
        'sort', type=str, help="how to sort the returned datasets. like: path_to_property:[asc|desc]", location="args")
    parser.add_argument(
        'filter', type=str, help="filter the search by a defined value in a path. e.g. path_to_property:value", location="args")

    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")
    es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('search in Index')
    def get(self, entity_type):
        """
        search on one given entity-index
        """
        print(type(self).__name__)
        retarray = []
        args = self.parser.parse_args()
        if entity_type in CONFIG.get("indices_list"):
            search = {}
            search["_source"] = {"excludes": self.excludes}
            if args.get("q") and not args.get("filter"):
                search["query"] = {"query_string": {"query": args.get("q")}}
            elif args.get("filter") and ":" in args.get("filter") and not args.get("q"):
                filter_fields = args.get("filter").split(":")
                search["query"] = {"match": {filter_fields[0] +
                                             ".keyword": ":".join(filter_fields[1:])}}
            elif args.get("q") and args.get("filter") and ":" in args.get("filter"):
                filter_fields = args.get("filter").split(":")
                search["query"] = {"bool": {"must": [{"query_string": {"query": args.get(
                    "q")}}, {"match": {filter_fields[0] + ".keyword":":".join(filter_fields[1:])}}]}}
            else:
                search["query"] = {"match_all": {}}
            if args.get("sort") and "|" in args.get("sort") and ("asc" in args.get("sort") or "desc" in args.get("sort")):
                sort_fields = args.get("sort").split("|")
                search["sort"] = [{sort_fields[0] + ".keyword":sort_fields[1]}]
            res = self.es.search(index=entity_type, body=search,
                                 size=args.get("size"), from_=args.get("from"))
            if "hits" in res and "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    retarray.append(hit.get("_source"))
        return self.response.parse(retarray, args.get("format"), "", flask.request)


# returns an single document given by index or id. if you use /index/search, then you can execute simple searches
@api.route(str('/<any({ent}):entity_type>/<string:id>'
               .format(ent=["resources"] + CONFIG.get("indices_list") + [" "])),
           methods=['GET'])
@api.param('entity_type', 'The name of the entity-type to access. Allowed Values: {}.'.format(CONFIG.get("indices_list")))
@api.param('id', 'The ID-String of the record to access. Possible Values (examples):118695940, 130909696')
class RetrieveDoc(LodResource):

    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")
    es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)
    parser = reqparse.RequestParser()
    parser.add_argument(
        'format', type=str, help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json", location="args")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('get Document out of an entity-type')
    def get(self, entity_type, id):
        """
        get a single record of an entity-index, or search for all records containing this record as an attribute via isAttr parameter
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
        typ = None
        for index in self.indices:
            if entity_type == self.indices[index]["index"]:
                typ = self.indices[index]["type"]
                break
        if entity_type == "resources":
            entity_type = "slub-resources"
            typ = "schemaorg"
        try:
            res = self.es.get(index=entity_type, doc_type=typ,
                              id=name, _source_exclude=self.excludes)
        except elasticsearch.ElasticsearchException:
            abort(404)
        retarray.append(res.get("_source"))
        return self.response.parse(retarray, args.get("format"), ending, flask.request)


@api.route('/search', methods=['GET', "PUT", "POST"])
class ESWrapper(LodResource):
    parser = reqparse.RequestParser()
    parser.add_argument('q', type=str, help="Lucene Query String Search Parameter", location="args")
    parser.add_argument(
        'format', type=str, help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json", location="args")
    parser.add_argument(
        'sort', type=str, help="how to sort the returned datasets. like: path_to_property:[asc|desc]", location="args")
    parser.add_argument(
        'size', type=int, help="Configure the maxmimum amount of hits to be returned", location="args")
    parser.add_argument(
        'from', type=int, help="Configure the offset from the frist result you want to fetch", location="args")
    parser.add_argument(
        'filter', type=str, help="filter the search by a defined value in a path. e.g. path_to_property:value", location="args")

    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")
    es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc("Search over all indices")
    def get(self):
        """
        search over all entity-indices
        """
        print(type(self).__name__)
        retarray = []
        args = self.parser.parse_args()
        search = {}
        search["_source"] = {"excludes": self.excludes}
        if args["q"] and not args["filter"]:
            search["query"] = {"query_string": {"query": args["q"]}}

        elif args["filter"] and ":" in args["filter"] and not args["q"]:
            filter_fields = args["filter"].split(":")
            search["query"] = {"match": {filter_fields[0] + ".keyword": ":".join(filter_fields[1:])}}

        elif args["q"] and args["filter"] and ":" in args["filter"]:
            filter_fields = args["filter"].split(":")
            search["query"] = {"bool": {"must": [{"query_string": {"query": args["q"]}}, {
                "match": {filter_fields[0] + ".keyword":":".join(filter_fields[1:])}}]}}

        else:
            search["query"] = {"match_all": {}}
        if args["sort"] and ":" in args["sort"] and ("asc" in args["sort"] or "desc" in args["sort"]):
            sort_fields = args["sort"].split(":")
            search["sort"] = [{sort_fields[0] + ".keyword":sort_fields[1]}]
        #    print(json.dumps(search,indent=4))
        searchindex = CONFIG.get("indices_list")
        if len(searchindex) > 1:
            searchindex = ','.join(searchindex)
        else:
            searchindex = searchindex[0]
        res = self.es.search(index=searchindex, body=search,
                             size=args["size"], from_=args["from"], _source_exclude=self.excludes)
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return self.response.parse(retarray, args.get("format"), "", flask.request)
