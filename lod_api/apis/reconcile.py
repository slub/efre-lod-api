import json
from elasticsearch import Elasticsearch
import flask
from flask_restplus import reqparse
from flask_restplus import Resource
from flask_restplus import Namespace
from flask_jsonpify import jsonpify

from lod_api import CONFIG
from lod_api.helper_functions import get_fields_with_subfields

api = Namespace("reconcile", path="/reconcile/",
                description="Openrefine Reconcilation and Data Extension Operations")


@api.route('/properties', methods=['GET'])
class ProposeProperties(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('queries', type=str,
                        help="OpenRefine Reconcilation API Call for Multiple Queries")
    parser.add_argument('callback', type=str, help="callback string")
    parser.add_argument('type', type=str, help="type string")
    parser.add_argument('limit', type=str, help="how many properties shall be returned")

    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")

    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Data-Extension-API.'
             'https://github.com/OpenRefine/OpenRefine/wiki/Data-Extension-API')
    def get(self):
        """
        Openrefine Data-Extension-API.
        https://github.com/OpenRefine/OpenRefine/wiki/Data-Extension-API
        """
        print(type(self).__name__)
        args = self.parser.parse_args()
        fields = set()
        limit = 256
        print(self.indices)
        typ = next(iter(self.indices))
        if args["type"]:
            typ = args["type"]
        if args["limit"]:
            try:
                limit = int(args["limit"])
            except:
                flask.abort(400)
        else:
            if typ in self.indices:
                fields = set()
                retDict = {}
                entity = self.indices[typ]["index"]
                # some python magic to get the first element of the dictionary in indices[typ]
                mapping = self.es.indices.get_mapping(index=entity)
                if entity in mapping:
                    retDict["type"] = typ
                    retDict["properties"] = []
                    for n, fld in enumerate(get_fields_with_subfields("", mapping[entity]["mappings"][self.indices[typ]["type"]]["properties"])):
                        if n < limit:
                            fields.add(fld)
                    for fld in fields:
                        retDict["properties"].append({"id": fld, "name": fld})
                return jsonpify(retDict)
            else:
                flask.abort(404)


@api.route('/suggest/entity', methods=['GET'])
class SuggestEntityEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('prefix', type=str, help='a string the user has typed')
    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API suggest Entity Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        args = self.parser.parse_args()
        search = self.es.search(index=",".join(CONFIG.get("indices_list")[:-1]), body={"query": {"match_phrase_prefix": {
                                "name": {"query": args["prefix"]}}}}, _source_include=["name", "@type"])
        result = {"result": []}
        for hit in search["hits"]["hits"]:
            _type = hit["_source"]["@type"]
            result["result"].append({"name":        hit["_source"]["name"],
                                     "description": hit["_source"]["@type"],
                                     "id":          self.indices[_type]["index"]+"/"+hit["_id"]})
        return jsonpify(result)


@api.route('/suggest/type', methods=['GET'])
class SuggestTypeEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('prefix', type=str, help='a string the user has typed')
    indices = CONFIG.get("indices_list")
    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API suggest Type Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        args = self.parser.parse_args()
        result = {"result": []}
        for k, v in self.indices.items():
            if args["prefix"] and v["description"].startswith(args["prefix"]) or not args["prefix"]:
                result["result"].append({"id": k,
                                         "name": v["index"],
                                         "description": v["description"]})
        return jsonpify(result)


@api.route('/suggest/property', methods=['GET'])
class SuggestPropertyEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('prefix', type=str, help='a string the user has typed')

    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)
    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API suggest Type Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        args = self.parser.parse_args()
        result = {"result": []}
        # some python magic to get the first element of the dictionary in indices[typ]
        mapping = self.es.indices.get_mapping(index=CONFIG.get("indices_list")[:-1])
        for index in mapping:
            # print(json.dumps(mapping[index]["mappings"]["schemaorg"],indent=4))
            fields = set()
            for fld in get_fields_with_subfields("", mapping[index]["mappings"]["schemaorg"]["properties"]):
                fields.add(fld)
        for fld in fields:
            if args["prefix"] and fld.startswith(args["prefix"]) or not args["prefix"]:
                result["result"].append({"id": fld, "name": fld})
        return jsonpify(result)

        for k, v in self.indices.items():
            if v["description"].startswith(args["prefix"]):
                result["result"].append({"id": k,
                                         "name": v["index"],
                                         "description": v["description"]})
        return jsonpify(result)


@api.route('/flyout/entity', methods=['GET'])
class FlyoutEntityEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('id', type=str, help='the identifier of the entity to render')
    es_host, es_port = CONFIG.get("es_host", "es_port")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API flyout Entity Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        arg = self.parser.parse_args()
        doc = self.es.get(index=arg["id"].split("/")[0], id=arg["id"].split("/")
                          [1], doc_type="schemaorg", _source_include="name")
        ret = {
            "html": "<p style=\"font-size: 0.8em; color: black;\">{}</p>".format(doc["_source"]["name"]), "id": arg["id"]}
        return jsonpify(ret)


@api.route('/flyout/type', methods=['GET'])
class FlyoutTypeEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('id', type=str, help='the identifier of the type to render')
    indices = CONFIG.get("indices_list")
    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API flyout Type Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        arg = self.parser.parse_args()
        ret = {"html": "<p style=\"font-size: 0.8em; color: black;\">{}</p>".format(
            self.indices[arg["id"]]["description"]), "id": arg["id"]}
        return jsonpify(ret)


@api.route('/flyout/property', methods=['GET'])
class FlyoutPropertyEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('id', type=str, help='the identifier of the property to render')
    @api.response(200, 'Success')
    @api.response(400, 'Check your Limit')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API flyout Property Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        arg = self.parser.parse_args()
        ret = {
            "html": "<p style=\"font-size: 0.8em; color: black;\">{}</p>".format(arg["id"]), "id": arg["id"]}
        return jsonpify(ret)


@api.route('/', methods=['GET', "POST"])
class apiData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('queries', type=str,
                        help="OpenRefine Reconcilation API Call for Multiple Queries")
    parser.add_argument('extend', type=str, help="extend your data with id and property")
    # parser.add_argument('query',type=str,help="OpenRefine Reconcilation API Call for Single Query") DEPRECATED
    parser.add_argument('callback', type=str, help="callback string")
    # parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json")
    es_host, es_port, excludes, indices, base = CONFIG.get("es_host", "es_port", "excludes", "indices", "base")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)
    @api.response(200, 'Success')
    @api.response(400, 'Check your JSON')
    @api.response(404, 'Record(s) not found')
    @api.doc('OpenRefine Reconcilation Service API. https://github.com/OpenRefine/OpenRefine/wiki/Reconciliation-Service-API')
    def get(self):
        """
        OpenRefine Reconcilation Service API. https://github.com/OpenRefine/OpenRefine/wiki/Reconciliation-Service-API
        """
        print(type(self).__name__)
        return self.reconcile()

    def post(self):
        """
        OpenRefine Reconcilation Service API. https://github.com/OpenRefine/OpenRefine/wiki/Reconciliation-Service-API
        """
        return self.reconcile()

    def reconcile(self):
        """
        OpenRefine Reconcilation Service API. https://github.com/OpenRefine/OpenRefine/wiki/Reconciliation-Service-API
        """

        doc = {}
        doc["name"] = "SLUB LOD reconciliation for OpenRefine"
        doc["identifierSpace"] = self.base
        doc["schemaSpace"] = "http://schema.org"
        doc["defaultTypes"] = []
        for k, v in self.indices.items():
            doc["defaultTypes"].append({"id": k, "name": v.get("description")})
        doc["view"] = {"url": self.base+"/{{id}}"}
        doc["preview"] = {"height": 100, "width": 320, "url": self.base+"/{{id}}.preview"}
        doc["extend"] = {"property_settings": [{"name": "limit", "label": "Limit", "type": "number", "default": 10, "help_text": "Maximum number of values to return per row (maximum: 1000)"},
                                               {"name": "type", "label": "Typ", "type": "string", "default": ",".join(CONFIG.get("indices_list")), "help_text": "Which Entity-Type to use, allwed values: {}".format(", ".join([x for x in self.indices]))}]}
        doc["extend"]["propose_properties"] = {
            "service_url": self.base,
            "service_path": "/reconcile/properties"
        }

        doc["suggest"] = {
            "property": {
                "flyout_service_path": "/reconcile/flyout/property?id=${id}",
                "service_path": "/reconcile/suggest/property",
                "service_url": self.base,
            },
            "type": {
                "flyout_service_path": "/reconcile/flyout/type?id=${id}",
                "service_path": "/reconcile/suggest/type",
                "service_url": self.base,
            },
            "entity": {
                "flyout_service_path": "/reconcile/flyout/entity?id=${id}",
                "service_path": "/reconcile/suggest/entity",
                "service_url": self.base,
            }
        }

        args = self.parser.parse_args()
        if args["extend"]:
            data = json.loads(args["extend"])
            if "ids" in data and "properties" in data:
                returnDict = {"rows": {}, "meta": []}
                for _id in data.get("ids"):
                    source = []
                    for prop in data.get("properties"):
                        source.append(prop.get("id"))
                    for index in self.indices:
                        if _id.split("/")[0] == self.indices[index]["index"]:
                            typ = self.indices[index]["type"]
                            break
                    es_data = self.es.get(index=_id.split(
                        "/")[0], doc_type="schemaorg", id=_id.split("/")[1], _source_include=source)
                    if "_source" in es_data:
                        returnDict["rows"][_id] = {}
                        for prop in data.get("properties"):
                            if prop["id"] in es_data["_source"]:
                                returnDict["rows"][_id][prop["id"]] = []
                                if isinstance(es_data["_source"][prop["id"]], str):
                                    returnDict["rows"][_id][prop["id"]].append(
                                        {"str": es_data["_source"][prop["id"]]})
                                elif isinstance(es_data["_source"][prop["id"]], list):
                                    for elem in es_data["_source"][prop["id"]]:
                                        if isinstance(elem, str):
                                            returnDict["rows"][_id][prop["id"]].append(
                                                {"str": elem})
                                        elif isinstance(elem, dict):
                                            if "@id" in elem and "name" in elem:
                                                returnDict["rows"][_id][prop["id"]].append(
                                                    {"id": "/".join(elem["@id"].split("/")[-2:]), "name": elem["name"]})
                                elif isinstance(es_data["_source"][prop["id"]], dict):
                                    if "@id" in es_data["_source"][prop["id"]] and "name" in es_data["_source"][prop["id"]]:
                                        returnDict["rows"][_id][prop["id"]].append(
                                            {"id": "/".join(es_data["_source"][prop["id"]]["@id"].split("/")[-2:]), "name": es_data["_source"][prop["id"]]["name"]})
                            else:
                                returnDict["rows"][_id][prop["id"]] = []
                for prop in data.get("properties"):
                    returnDict["meta"].append({"id": prop["id"], "name": prop["id"], "type": {
                                              "name": "Thing", "id": "http://schema.org/Thing"}})
                return jsonpify(returnDict)
            else:
                flask.abort(400)
        if not args["queries"]:
            return jsonpify(doc)
        returndict = {}
        inp = json.loads(args["queries"])
        for query in inp:
            if isinstance(inp[query], dict) and "query" in inp[query]:
                returndict[query] = {}
                returndict[query]["result"] = list()
                if inp[query].get("limit"):
                    size = inp[query].get("limit")
                else:
                    size = 10
                if inp[query].get("type") and inp[query].get("type") in self.indices:
                    index = self.indices[inp[query].get("type")].get("index")
                elif len(CONFIG.get("indices_list")) > 2:
                    index = ",".join(CONFIG.get("indices_list")[:-1])
                elif len(CONFIG.get("indices_list")) <= 2:
                    index = CONFIG.get("indices_list")[0]
                search = {}
                search["_source"] = {"excludes": self.excludes}
                if "properties" in inp[query]:
                    searchtype = "should"
                else:
                    searchtype = "must"
                search["query"] = {"bool": {searchtype: [
                    {"query_string": {"query": "\""+inp[query]["query"]+"\""}}]}}
                if inp[query].get("properties") and isinstance(inp[query]["properties"], list):
                    for prop in inp[query]["properties"]:
                        search["query"]["bool"]["should"].append(
                            {"match": {prop.get("pid")+".keyword": prop.get("v")}})
                res = self.es.search(index=index, body=search, size=size)
                if "hits" in res and "hits" in res["hits"]:
                    for hit in res["hits"]["hits"]:
                        resulthit = {}
                        resulthit["type"] = []
                        # resulthit["type"].append({"id":hit["_source"]["@type"],"name":types.get(hit["_source"]["@type"])})
                        resulthit["type"] = doc["defaultTypes"]
                        if "name" in hit["_source"]:
                            resulthit["name"] = hit["_source"]["name"]
                        elif "dct:title" in hit["_source"]:
                            resulthit["name"] = hit["_source"]["dct:title"]
                        resulthit["score"] = hit["_score"]
                        resulthit["id"] = hit["_index"] + "/" + hit["_id"]
                        if inp[query]["query"].lower() in resulthit["name"].lower() or resulthit["name"].lower() in inp[query]["query"].lower():
                            resulthit["match"] = True
                        else:
                            resulthit["match"] = False
                        returndict[query]["result"].append(resulthit)
                if (isinstance(returndict[query]["result"], list)
                    and len(returndict[query]["result"]) > 1
                        and returndict[query]["result"][0]["score"] > returndict[query]["result"][1]["score"]*2):
                    returndict[query]["result"][0]["match"] = True
        return jsonpify(returndict)
