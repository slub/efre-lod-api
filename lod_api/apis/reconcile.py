import json
from elasticsearch import Elasticsearch
import flask
from flask_restx import reqparse
from flask_restx import Resource
from flask_restx import Namespace
from flask_jsonpify import jsonpify

from lod_api import CONFIG
from lod_api.tools.helper import get_fields_with_subfields
from lod_api.tools.helper import isint
from lod_api.tools.helper import getNestedJsonObject

api = Namespace("reconcile", path="/reconcile/",
                description="Openrefine Reconcilation and Data Extension Operations")


def data_to_preview(data, request):
    """ Takes `data` as a dictionary and generates a html
        preview with the most important values out off `data`
        deciding on its entity type.
        The preview contains:
          - The ID of the dataset [`@id`]
          - The name or title of the dataset [`name`]/[`dct:title`]
          - The entity type of the dataset [`@type`]/[`rdfs:ch_type`]
        as well as additional information in the `free_content`-field, e.g.
          - `birthDate` if the type is a person
    """
    preview_mapping, pv_html = CONFIG.get(
        "indices", "openrefine_preview_html_text")

    elem = data[0]

    _id = elem.get("@id")
    endpoint = _id.split("/")[-2] + "/" + _id.split("/")[-1]

    if elem.get("@type"):
        display_type = elem.get("@type")
    elif elem.get("rdfs:ch_type"):
        display_type = elem.get("rdfs:ch_type")["@id"]

    for mapping_type_key in preview_mapping:
        if mapping_type_key in display_type:
            mapping_type = mapping_type_key
            break

    pv_free_content = preview_mapping[mapping_type]["openrefine_preview_free_content"]

    free_content = ""
    if isinstance(pv_free_content, list):
        for free_content_field in pv_free_content:
            if ">" in free_content_field:
                free_content = getNestedJsonObject(elem, free_content_field)
            else:
                free_content = elem.get(free_content_field)
            if free_content:
                break
    elif isinstance(pv_free_content, str) and ">" in pv_free_content:
        free_content = getNestedJsonObject(elem, pv_free_content)
    elif isinstance(pv_free_content, str):
        free_content = elem.get(pv_free_content)

    label = elem.get(preview_mapping[mapping_type]["label_field"])

    html = pv_html.format(id=_id, title=label, endpoint=endpoint,
                          content=free_content, typ=display_type)

    response = flask.Response(html, mimetype='text/html; charset=UTF-8')
    # Optionally:
    # send the Response through _encode() fo the the Output class to
    # be enable gzip-compression if defined in the request header
    return response


@api.route('/properties', methods=['GET'])
class ProposeProperties(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('queries', type=str,
                        help="OpenRefine Reconcilation API Call for Multiple Queries")
    parser.add_argument('type', type=str, help="type string")
    parser.add_argument('limit', type=str,
                        help="how many properties shall be returned")

    es_host, es_port, excludes, indices = CONFIG.get(
        "es_host", "es_port", "excludes", "indices")

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
        # some python magic to get the first element of the dictionary in indices[typ]
        typ = next(iter(self.indices))
        if args["type"]:
            typ = args["type"]
        if args["limit"]:
            if isint(args["limit"]):
                limit = int(args["limit"])
            else:
                flask.abort(400)
        if typ in self.indices:
            fields = set()
            retDict = {}
            entity = self.indices[typ]["index"]
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
        flask.abort(404)


@api.route('/suggest/entity', methods=['GET'])
class SuggestEntityEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('prefix', type=str, help='a string the user has typed')
    es_host, es_port, indices, indices_list = CONFIG.get(
        "es_host", "es_port", "indices", "indices_list")
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

        name_Fields = set()
        for config_index in self.indices:
            name_Fields.add(self.indices[config_index]["label_field"])
        args = self.parser.parse_args()
        search = self.es.search(index=",".join(self.indices_list), body={"query": {"bool": {"should": [{"match_phrase_prefix": {
                                str(namefield): args["prefix"]}} for namefield in name_Fields]}}}, _source_include=list(name_Fields)+["@type"])
        result = {"result": []}
        for hit in search["hits"]["hits"]:
            for name_Field in name_Fields:
                if hit["_source"].get(name_Field):
                    result["result"].append({"name": hit["_source"][name_Field],
                                             "description": hit["_source"]["@type"],
                                             "id": hit["_index"] + "/" + hit["_id"]})
        return jsonpify(result)


@api.route('/suggest/type', methods=['GET'])
class SuggestTypeEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('prefix', type=str, help='a string the user has typed')
    indices = CONFIG.get("indices")
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
    es_host, es_port, excludes, indices, indices_list = CONFIG.get(
        "es_host", "es_port", "excludes", "indices", "indices_list")
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
        mapping = self.es.indices.get_mapping(index=self.indices_list)
        for index in mapping:
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
    parser.add_argument(
        'id', type=str, help='the identifier of the entity to render')
    es_host, es_port, indices = CONFIG.get("es_host", "es_port", "indices")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(400, 'Check your ID')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API flyout Entity Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        arg = self.parser.parse_args()
        if arg["id"]:
            # GET parameter `id` looks like e.g
            #     "persons/12345678"
            # -> index is just "persons" in this case
            index = arg["id"].split("/")[0]

            # -> es_id is the elastic search identifier after "/"
            es_id = arg["id"].split("/")[1]

            doc_type = ""
            name_Field = ""
            for config_index in self.indices:
                if index == self.indices[config_index]["index"]:
                    doc_type = self.indices[config_index]["type"]
                    name_Field = self.indices[config_index]["label_field"]
                    break

            doc = self.es.get(index=index, id=es_id,
                              doc_type=doc_type, _source_include=name_Field)
            ret = {
                "html": "<p style=\"font-size: 0.8em; color: black;\">{}</p>".format(doc["_source"].get(name_Field)), "id": arg["id"]}
            return jsonpify(ret)
        else:
            flask.abort(400)


@api.route('/flyout/type', methods=['GET'])
class FlyoutTypeEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        'id', type=str, help='the identifier of the type to render')
    indices = CONFIG.get("indices")
    @api.response(200, 'Success')
    @api.response(400, 'Check your ID')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API flyout Type Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        arg = self.parser.parse_args()
        if arg["id"]:
            ret = {"html": "<p style=\"font-size: 0.8em; color: black;\">{}</p>".format(
                self.indices[arg["id"]]["description"]), "id": arg["id"]}
            return jsonpify(ret)
        else:
            flask.abort(400)


@api.route('/flyout/property', methods=['GET'])
class FlyoutPropertyEntryPoint(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument(
        'id', type=str, help='the identifier of the property to render')

    @api.response(200, 'Success')
    @api.response(404, 'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Suggest-API flyout Property Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API')
    def get(self):
        """
        Openrefine Suggest-API suggest Entry Point. https://github.com/OpenRefine/OpenRefine/wiki/Suggest-API
        """
        arg = self.parser.parse_args()
        if arg["id"]:
            ret = {"html": "<p style=\"font-size: 0.8em; color: black;\">{}</p>".format(
                arg["id"]), "id": arg["id"]}
            return jsonpify(ret)
        else:
            flask.abort(400)


@api.route('/', methods=['GET', "POST"])
class apiData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('queries', type=str,
                        help="OpenRefine Reconcilation API Call for Multiple Queries")
    parser.add_argument('extend', type=str,
                        help="extend your data with id and property")
    # parser.add_argument('query',type=str,help="OpenRefine Reconcilation API Call for Single Query") DEPRECATED
    es_host, es_port, excludes, indices, base, doc, indices_list = CONFIG.get(
        "es_host", "es_port", "excludes", "indices", "base", "reconcile_doc", "indices_list")
    es = Elasticsearch([{'host': es_host}], port=es_port, timeout=10)
    for k, v in indices.items():
        doc["defaultTypes"].append({"id": k, "name": v.get("description")})
    doc["extend"]["property_settings"][1]["default"] = ",".join(indices_list)
    doc["extend"]["property_settings"][1]["help_text"] = doc["extend"]["property_settings"][1]["help_text"] + \
        ", ".join([x for x in indices])
    doc["identifierSpace"] = base
    doc["view"]["url"] = doc["view"]["url"].replace("base", base)
    doc["preview"]["url"] = doc["preview"]["url"].replace("base", base)
    doc["extend"]["propose_properties"]["service_url"] = doc["extend"]["propose_properties"]["service_url"].replace(
        "base", base)
    doc["suggest"]["property"]["service_url"] = doc["suggest"]["property"]["service_url"].replace(
        "base", base)
    doc["suggest"]["type"]["service_url"] = doc["suggest"]["type"]["service_url"].replace(
        "base", base)
    doc["suggest"]["entity"]["service_url"] = doc["suggest"]["entity"]["service_url"].replace(
        "base", base)

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
        args = self.parser.parse_args()
        if args["extend"]:
            return self.extend(args)
        if not args["queries"]:
            return jsonpify(self.doc)
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
                elif len(self.indices_list) > 1:
                    index = ",".join(self.indices_list)
                elif len(self.indices_list) <= 1:
                    index = self.indices_list[0]
                search = {}
                search["_source"] = {"excludes": self.excludes}
                if "properties" in inp[query]:
                    searchtype = "should"
                else:
                    searchtype = "must"
                search["query"] = {"bool": {searchtype: [
                    {"query_string": {"query": "\"" + inp[query]["query"] + "\""}}]}}
                if inp[query].get("properties") and isinstance(inp[query]["properties"], list):
                    for prop in inp[query]["properties"]:
                        search["query"]["bool"]["should"].append(
                            {"match": {prop.get("pid") + ".keyword": prop.get("v")}})
                res = self.es.search(index=index, body=search, size=size)
                if "hits" in res and "hits" in res["hits"]:
                    for hit in res["hits"]["hits"]:
                        resulthit = {}
                        resulthit["type"] = []
                        resulthit["type"] = self.doc["defaultTypes"]
                        label_field = None
                        for config_index in self.indices:
                            if hit["_index"] in self.indices[config_index]["index"]:
                                label_field = self.indices[config_index]["label_field"]
                                break
                        if label_field and label_field in hit["_source"]:
                            resulthit["name"] = hit["_source"][label_field]
                            resulthit["score"] = hit["_score"]
                            resulthit["id"] = hit["_index"] + "/" + hit["_id"]
                            if inp[query]["query"].lower() in resulthit["name"].lower() or resulthit["name"].lower() in inp[query]["query"].lower():
                                resulthit["match"] = True
                            else:
                                resulthit["match"] = False
                            returndict[query]["result"].append(resulthit)
                if (isinstance(returndict[query]["result"], list)
                    and len(returndict[query]["result"]) > 1
                        and returndict[query]["result"][0]["score"] > returndict[query]["result"][1]["score"] * 2):
                    returndict[query]["result"][0]["match"] = True
        return jsonpify(returndict)

    def extend(self, args):
        """
        OpenRefine Data Extension API https://github.com/OpenRefine/OpenRefine/wiki/Data-Extension-API
        """
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
                    else:
                        typ = "schemaorg"
                es_data = self.es.get(index=_id.split(
                    "/")[0], doc_type=typ, id=_id.split("/")[1], _source_include=source)
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
                                    # https://giphy.com/gifs/stress-i-need-a-drink-brain-explode-2rqEdFfkMzXmo/fullscreen
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
