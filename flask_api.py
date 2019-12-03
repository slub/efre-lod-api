# use `run_via_bjoern.py` to run this in production behind an nginx
# use `python3 flask_api.py` to run during debugging
#
# inspired by lobid.org
#
#

import sys
import json
import gzip
from io import BytesIO
from io import StringIO
from flask import Flask
from flask import jsonify
from flask import abort
from flask import Response
from flask import redirect
from flask import request
from flask import render_template
from flask import Blueprint
from flask_restplus import reqparse
from flask_restplus import Resource
from flask_restplus import Api
from flask_jsonpify import jsonpify
from elasticsearch import Elasticsearch
from werkzeug.contrib.fixers import ProxyFix



with open("apiconfig.json") as data_file:
    config=json.load(data_file)

host=config.get("host")
port=config.get("port")
bibsource_host=config.get("bibsource_host")
bibsource_port=config.get("bibsource_port")

app=Flask(__name__)


swagger_api = Blueprint("swagger", __name__,
                        static_folder='doc',
                        template_folder='templates')

app.register_blueprint(swagger_api)


# app.wsgi_app = ProxyFix(app.wsgi_app)
# @app.route('/')
# def get():
#     return redirect(config.get("storydocpage"))


api = Api(  app,
            title=config.get("apititle"),
            default=config.get("apiname"),
            default_label=config.get("default_label"),
            default_mediatype=config.get("default_mediatype"),
            contact=config.get("contact"),
            contact_email=config.get("contact_email"),
            doc='/doc/api/')

@api.documentation
def render_swagger_page():
    return(render_template(['slub-swagger-ui.html', 'swagger-ui.html'],
           title="API - SLUB - LOD API documentation",
           specs_url=api.specs_url))

es=Elasticsearch([{'host':host}],port=port,timeout=10)
bibsource_es=Elasticsearch([{'host':bibsource_host}],port=bibsource_port,timeout=5)

indices = config["indices"]
authorities=config.get("authorities")
excludes=config["excludes"]
def get_type_or_class_name(var) -> str:
    if type(var).__name__ == 'type':
        return var.__name__
    else:
        return type(var).__name__

@api.errorhandler(Exception)
def generic_exception_handler(e: Exception):
    exc_type, exc_value, exc_traceback = sys.exc_info()

    if exc_traceback:
        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'lineno': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': get_type_or_class_name(exc_type),
            'message': str(exc_value),
        }
        return {'message': "Internal Server Error: "+traceback_details['message']}, 500
    else:
        return {'message': 'Internal Server Error'}, 500

def get_indices():
    ret=set()
    for obj in [x for x in indices.values()]:
        ret.add(obj.get("index"))
    #print(list(ret))
    return list(ret)+[" "] # BUG Last Element doesn't work. So we add a whitespace Element which won't work, instead of an Indexname


# generic output class
class Output:
    rdflib = __import__("rdflib")
    # gzip,

    def __init__(self):
        self.format = {
                "nt":     self.convert_data_to_nt,
                "ttl":    self.convert_data_to_ttl,
                "rdf":    self.convert_data_to_rdf,
                "nq":     self.convert_data_to_nq,
                "jsonl":  self.convert_data_to_jsonl
                }
        # preview missing

        self.mediatype = {
                "application/json":        "json",
                "application/ld+json":     "jsonl",
                "application/x-jsonlines": "jsonl",
                "application/n-triples":   "nt",
                "application/rdf+xml":     "rdf",
                "text/turtle":             "ttl",
                "application/n-quads":     "nq"
                }

    def __gunzip__(self, data):
        """ build a Response-Object which is returned to the client
            first re-serialize the data to other RDF implementations if needed
            gzip-complress if the client supports it via Accepted-Encoding
        """
        gzip_buffer = BytesIO()
        with gzip.open(gzip_buffer,mode="wb",compresslevel=6) as gzip_file:
            gzip_file.write(data.data)
        data.data=gzip_buffer.getvalue()
        data.headers['Content-Encoding'] = 'gzip'
        data.headers['Vary'] = 'Accept-Encoding'
        data.headers['Content-Length'] = data.content_length
        return data

    def __encode__(self, data, res):
        """ use gzip to encode requested data if the
            Accept-Encoding Header is set accordingly
        """
        if "gzip" in request.headers.get("Accept-Encoding"):
            return self.__gunzip__(res)
        else:
            return res

    def __parse_json__(self, data):
        """ use RDFlib to parse json """
        g = self.rdflib.ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        return g


    def parse(self, data, format, file_ext, request):
        """ Dateiendung vor Formatparameter vor Request-Header

        """
        retformat = ""
        # parse request-header and fileending
        if request.headers.get("Content-Type"):
            encoding = request.headers.get("Content-Type")
        elif request.headers.get("Accept"):
            encoding = request.headers.get("Accept")

        file_ext_avail = [key for key in self.format]
        mediatype_avail = [key for key in self.mediatype]

        if file_ext and file_ext in file_ext_avail:
            retformat = file_ext
        elif not file_ext and format in file_ext_avail:
            retformat = format
        elif encoding in mediatype_avail:
            retformat=self.mediatype[encoding]
        else:
            retformat="json"

        print(retformat)

        ret = None
        if not data:    # returns 404 if data not set
            abort(404)

        # check out the format string for ?format= or Content-Type Headers
        try:
            return self.format[retformat](data, request)
        except KeyError:
            return self.__encode__(data, jsonify(data))



    @api.representation("application/n-triples")
    def convert_data_to_nt(self, data, request):
        data_out = self.__parse_json__(data).serialize(format="nt").decode('utf-8')
        res = Response(data_out, mimetype='application/n-triples')
        return self.__encode__(data_out, res)

    @api.representation("application/rdf+xml")
    def convert_data_to_rdf(self, data, request):
        data_out = self.__parse_json__(data).serialize(format="application/rdf+xml").decode('utf-8')
        res = Response(data_out, mimetype='application/rdf+xml')
        return self.__encode__(data_out, res)

    @api.representation("text/turtle")
    def convert_data_to_ttl(self, data, request):
        data_out = self.__parse_json__(data).serialize(format="turtle").decode('utf-8')
        res = Response(data_out, mimetype='text/turtle')
        return self.__encode__(data_out, res)

    @api.representation("application/n-quads")
    def convert_data_to_nq(self, data, request):
        data_out = self.__parse_json__(data).serialize(format="nquads").decode('utf-8')
        res = Response(data_out, mimetype='application/n-quads')
        return self.__encode__(data_out, res)

    @api.representation("application/x-jsonlines")
    def convert_data_to_jsonl(self, data, request):
        data_out = ""
        if isinstance(data, list):
            for item in data:
                data_out += json.dumps(item,indent=None)+"\n"
        elif isinstance(data, dict):
            data_out += json.dumps(data,indent=None)+"\n"

        res = output_jsonl(Response(data_out, mimetype='application/x-jsonlines'))
        return self.__encode__(data_out, res)


class Output_with_preview(Output):
    """ Extend the standard Output class with a function that renders
        an HTML preview used by the OpenRefine Reconciliation API

        The preview is called by simply appending ".preview" to the
        requested dataset.
    """
    def __init__(self):
        super().__init__()
        self.format["preview"] = self.data_to_preview

    def data_to_preview(self, data, _):
        for elem in data:
            _id=elem.get("@id")
            endpoint = _id.split("/")[-2] + "/" + _id.split("/")[-1]

            if "name" in elem:
                title = elem.get("name")
            else:
                title = elem.get("dct:title")
            if elem.get("@type"):
                typ = elem.get("@type")
            elif elem.get("rdfs:ch_type"):
                typ=elem.get("rdfs:ch_type")["@id"]

            free_field=""
            print(typ)

            if typ == "http://schema.org/Person":
                free_field = elem.get("birthDate")
            elif typ == "http://schema.org/CreativeWork" or typ.startswith("bibo"):
                if "author" in elem:
                    free_field = elem.get("author")[0]["name"]
                elif not "author" in elem and "contributor" in elem:
                    free_field = elem.get("contributor")[0]["name"]
                elif "bf:contribution" in elem:
                    free_field = elem.get("bf:contribution")[0]["bf:agent"]["rdfs:ch_label"]
            elif typ == "http://schema.org/Place":
                free_field = elem.get("adressRegion")
            elif typ == "http://schema.org/Organization":
                free_field = elem.get("location").get("name")
        html = """<html><head><meta charset=\"utf-8\" /></head>
                  <body style=\"margin: 0px; font-family: Arial; sans-serif\">
                  <div style=\"height: 100px; width: 320px; overflow: hidden; font-size: 0.7em\">


                      <div style=\"margin-left: 5px;\">
                          <a href=\"{id}\" target=\"_blank\" style=\"text-decoration: none;\">{title}</a>
                          <span style=\"color: #505050;\">({endpoint})</span>
                          <p>{content}</p>
                          <p>{typ}</p>
                      </div>

                  </div>
                  </body>
                  </html>
                  """.format(id=_id,
                               title=title,
                               endpoint=endpoint,
                               content=free_field,
                               typ=typ)
        return Response(html ,mimetype='text/html; charset=UTF-8')

output = Output_with_preview()


@api.route('/<any({ent}):entity_type>/search'.format(ent=get_indices()),methods=['GET'])
@api.param('entity_type','The name of the entity-type to access. Allowed Values: {}.'.format(get_indices()))
class searchDoc(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('q',type=str,help="Lucene Query String Search Parameter",location="args")
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('size',type=int,help="Configure the maxmimum amount of hits to be returned",location="args",default=100)
    parser.add_argument('from',type=int,help="Configure the offset from the frist result you want to fetch",location="args",default=0)
    parser.add_argument('sort',type=str,help="how to sort the returned datasets. like: path_to_property:[asc|desc]",location="args")
    parser.add_argument('filter',type=str,help="filter the search by a defined value in a path. e.g. path_to_property:value",location="args")

    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc('search in Index')
    def get(self,entity_type):
        """
        search on one given entity-index
        """
        print(type(self).__name__)
        print(app.url_map)
        retarray=[]
        args=self.parser.parse_args()
        if entity_type in get_indices():
                search={}
                search["_source"]={"excludes":excludes}
                if args.get("q") and not args.get("filter"):
                    search["query"]={"query_string" : {"query": args.get("q")}}
                elif args.get("filter") and ":" in args.get("filter") and not  args.get("q"):
                    filter_fields=args.get("filter").split(":")
                    search["query"]={"match":{filter_fields[0]+".keyword":":".join(filter_fields[1:])}}
                elif  args.get("q") and args.get("filter") and ":" in args.get("filter"):
                    filter_fields=args.get("filter").split(":")
                    search["query"]={"bool":{"must":[{"query_string":{"query": args.get("q")}},{"match":{filter_fields[0]+".keyword":":".join(filter_fields[1:])}}]}}
                else:
                    search["query"]={"match_all":{}}
                if args.get("sort") and "|" in args.get("sort") and ( "asc" in args.get("sort") or "desc" in args.get("sort") ):
                    sort_fields=args.get("sort").split("|")
                    search["sort"]=[{sort_fields[0]+".keyword":sort_fields[1]}]
                res=es.search(index=entity_type,body=search,size=args.get("size"), from_=args.get("from"))
                if "hits" in res and "hits" in res["hits"]:
                    for hit in res["hits"]["hits"]:
                        retarray.append(hit.get("_source"))
        return output.parse(retarray,args.get("format"),"",request)

#returns an single document given by index or id. if you use /index/search, then you can execute simple searches
@api.route(str('/<any({ent}):entity_type>/<string:id>'.format(ent=["resources"]+get_indices())),methods=['GET'])
@api.param('entity_type','The name of the entity-type to access. Allowed Values: {}.'.format(get_indices()))
@api.param('id','The ID-String of the record to access. Possible Values (examples):118695940, 130909696')
class RetrieveDoc(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")

    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc('get Document out of an entity-type')
    def get(self,entity_type,id):
        """
        get a single record of an entity-index, or search for all records containing this record as an attribute via isAttr parameter
        """
        print(type(self).__name__)
        retarray=[]
        args=self.parser.parse_args()
        name=""
        ending=""
        if "." in id:
            dot_fields=id.split(".")
            name=dot_fields[0]
            ending=dot_fields[1]
        else:
            name=id
            ending=""
        try:
            typ=None
            for index in indices:
                if entity_type==indices[index]["index"]:
                    typ=indices[index]["type"]
                    break
            if entity_type=="resources":
                entity_type="slub-resources"
                typ="schemaorg"
            res=es.get(index=entity_type,doc_type=typ,id=name,_source_exclude=excludes)
            retarray.append(res.get("_source"))
        except:
            abort(404)
        return output.parse(retarray,args.get("format"),ending,request)

def get_fields_with_subfields(prefix,data):
    for k,v in data.items():
        yield prefix+k
        if "properties" in v:
            for item in get_fields_with_subfields(k+".",v["properties"]):
                yield item

@api.route('/reconcile/properties',methods=['GET'])
class proposeProperties(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('queries',type=str,help="OpenRefine Reconcilation API Call for Multiple Queries")
    #parser.add_argument('query',type=str,help="OpenRefine Reconcilation API Call for Single Query") DEPRECATED
    parser.add_argument('callback',type=str,help="callback string")
    parser.add_argument('type',type=str,help="type string")
    parser.add_argument('limit',type=str,help="how many properties shall be returned")
    @api.response(200,'Success')
    @api.response(400,'Check your Limit')
    @api.response(404,'Type not found')
    @api.expect(parser)
    @api.doc('Openrefine Data-Extension-API. https://github.com/OpenRefine/OpenRefine/wiki/Data-Extension-API')

    def get(self):
        """
        Openrefine Data-Extension-API. https://github.com/OpenRefine/OpenRefine/wiki/Data-Extension-API
        """
        print(type(self).__name__)
        args=self.parser.parse_args()
        fields=set()
        limit=256
        print(indices)
        typ=next(iter(indices))
        if args["type"]:
            typ=args["type"]
        if args["limit"]:
            try:
                limit=int(args["limit"])
            except:
                abort(400)
        else:
            if typ in indices:
                fields=set()
                retDict={}
                entity=indices[typ]["index"]
                mapping=es.indices.get_mapping(index=entity) # some python magic to get the first element of the dictionary in indices[typ]
                if entity in mapping:
                    retDict["type"]=typ
                    retDict["properties"]=[]
                    for n,fld in enumerate(get_fields_with_subfields("",mapping[entity]["mappings"][indices[typ]["type"]]["properties"])):
                        if n<limit:
                            fields.add(fld)
                    for fld in fields:
                        retDict["properties"].append({"id":fld,"name":fld})
                return jsonpify(retDict)
            else:
                abort(404)




@api.route('/reconcile',methods=['GET', "POST"])
class reconcileData(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('queries',type=str,help="OpenRefine Reconcilation API Call for Multiple Queries")
    parser.add_argument('extend',type=str,help="extend your data with id and property")
    #parser.add_argument('query',type=str,help="OpenRefine Reconcilation API Call for Single Query") DEPRECATED
    parser.add_argument('callback',type=str,help="callback string")
    #parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json")
    @api.response(200,'Success')
    @api.response(400,'Check your JSON')
    @api.response(404,'Record(s) not found')
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

        doc={}
        doc["name"]="SLUB LOD reconciliation for OpenRefine"
        doc["identifierSpace"]=config["base"]
        doc["schemaSpace"]="http://schema.org"
        doc["defaultTypes"]=[]
        for k,v in indices.items():
            doc["defaultTypes"].append({"id":k,"name":v.get("description")})
        doc["view"]={"url":config["base"]+"/{{id}}"}
        doc["preview"]={ "height": 100, "width": 320, "url":config["base"]+"/{{id}}.preview" }
        doc["extend"]={"property_settings": [ { "name": "limit", "label": "Limit", "type": "number", "default": 10, "help_text": "Maximum number of values to return per row (maximum: 1000)" },
                                              { "name": "type", "label": "Typ", "type": "string", "default": ",".join(get_indices()), "help_text": "Which Entity-Type to use, allwed values: {}".format(", ".join([x for x in indices])) }]}
        doc["extend"]["propose_properties"]={
            "service_url": config["base"],
            "service_path": "/reconcile/properties"
        }

        args=self.parser.parse_args()
        if args["extend"]:
            data=json.loads(args["extend"])
            if "ids" in data and "properties" in data:
                returnDict={"rows":{},"meta":[]}
                for _id in data.get("ids"):
                    source=[]
                    for prop in data.get("properties"):
                        source.append(prop.get("id"))
                    for index in indices:
                        if _id.split("/")[0]==indices[index]["index"]:
                            typ=indices[index]["type"]
                            break
                    es_data=es.get(index=_id.split("/")[0],doc_type="schemaorg",id=_id.split("/")[1],_source_include=source)
                    if "_source" in es_data:
                        returnDict["rows"][_id]={}
                        for prop in data.get("properties"):
                            if prop["id"] in es_data["_source"]:
                                returnDict["rows"][_id][prop["id"]]=[]
                                if isinstance(es_data["_source"][prop["id"]],str):
                                    returnDict["rows"][_id][prop["id"]].append({"str":es_data["_source"][prop["id"]]})
                                elif isinstance(es_data["_source"][prop["id"]],list):
                                    for elem in es_data["_source"][prop["id"]]:
                                        if isinstance(elem,str):
                                            returnDict["rows"][_id][prop["id"]].append({"str":elem})
                                        elif isinstance(elem,dict):
                                            if "@id" in elem and "name" in elem:
                                                returnDict["rows"][_id][prop["id"]].append({"id":"/".join(elem["@id"].split("/")[-2:]),"name":elem["name"]})
                                elif isinstance(es_data["_source"][prop["id"]],dict):
                                    if "@id" in es_data["_source"][prop["id"]] and "name" in es_data["_source"][prop["id"]]:
                                        returnDict["rows"][_id][prop["id"]].append({"id":"/".join(es_data["_source"][prop["id"]]["@id"].split("/")[-2:]),"name":es_data["_source"][prop["id"]]["name"]})
                            else:
                                returnDict["rows"][_id][prop["id"]]=[]
                for prop in data.get("properties"):
                    returnDict["meta"].append({"id":prop["id"],"name":prop["id"],"type":{"name":"Thing","id":"http://schema.org/Thing"}})
                return jsonpify(returnDict)
            else:
                abort(400)
        if not args["queries"]:
            return jsonpify(doc)
        returndict={}
        inp= json.loads(args["queries"])
        for query in inp:
            if isinstance(inp[query],dict) and "query" in inp[query]:
                returndict[query]={}
                returndict[query]["result"]=list()
                if inp[query].get("limit"):
                    size=inp[query].get("limit")
                else:
                    size=10
                if inp[query].get("type") and inp[query].get("type") in indices:
                    index=indices[inp[query].get("type")].get("index")
                elif len(get_indices())>2:
                    index=",".join(get_indices()[:-1])
                elif len(get_indices())<=2:
                    index=get_indices()[0]
                search={}
                search["_source"]={"excludes":excludes}
                if "properties" in inp[query]:
                    searchtype="should"
                else:
                    searchtype="must"
                search["query"]={"bool":{searchtype:[{"query_string" : {"query":"\""+inp[query]["query"]+"\""}}]}}
                if inp[query].get("properties") and isinstance(inp[query]["properties"],list):
                    for prop in inp[query]["properties"]:
                        search["query"]["bool"]["should"].append({"match": {prop.get("pid")+".keyword": prop.get("v")}})
                res=es.search(index=index,body=search,size=size)
                if "hits" in res and "hits" in res["hits"]:
                    for hit in res["hits"]["hits"]:
                        resulthit={}
                        resulthit["type"]=[]
                        #resulthit["type"].append({"id":hit["_source"]["@type"],"name":types.get(hit["_source"]["@type"])})
                        resulthit["type"]=doc["defaultTypes"]
                        if "name" in hit["_source"]:
                            resulthit["name"]=hit["_source"]["name"]
                        elif "dct:title" in hit["_source"]:
                            resulthit["name"]=hit["_source"]["dct:title"]
                        resulthit["score"]=hit["_score"]
                        resulthit["id"]=hit["_index"]+"/"+hit["_id"]
                        if inp[query]["query"].lower() in resulthit["name"].lower() or resulthit["name"].lower() in inp[query]["query"].lower():
                            resulthit["match"]=True
                        else:
                            resulthit["match"]=False
                        returndict[query]["result"].append(resulthit)
                if isinstance(returndict[query]["result"],list) and len(returndict[query]["result"])>1 and returndict[query]["result"][0]["score"]>returndict[query]["result"][1]["score"]*2:
                    returndict[query]["result"][0]["match"]=True
        return jsonpify(returndict)



@api.route('/search',methods=['GET',"PUT", "POST"])
class ESWrapper(Resource):
    parser = reqparse.RequestParser()
    parser.add_argument('q',type=str,help="Lucene Query String Search Parameter",location="args")
    parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
    parser.add_argument('sort',type=str,help="how to sort the returned datasets. like: path_to_property:[asc|desc]",location="args")
    parser.add_argument('size',type=int,help="Configure the maxmimum amount of hits to be returned",location="args")
    parser.add_argument('from',type=int,help="Configure the offset from the frist result you want to fetch",location="args")
    parser.add_argument('filter',type=str,help="filter the search by a defined value in a path. e.g. path_to_property:value",location="args")

    @api.response(200,'Success')
    @api.response(404,'Record(s) not found')
    @api.expect(parser)
    @api.doc("Search over all indices")
    def get(self):
        """
        search over all entity-indices
        """
        print(type(self).__name__)
        retarray=[]
        args=self.parser.parse_args()
        search={}
        search["_source"]={"excludes":excludes}
        if args["q"] and not args["filter"]:
            search["query"]={"query_string" : {"query":args["q"]}}
        elif args["filter"] and ":" in args["filter"] and not args["q"]:
            filter_fields=args["filter"].split(":")
            search["query"]={"match":{filter_fields[0]+".keyword":":".join(filter_fields[1:])}}
        elif args["q"] and args["filter"] and ":" in args["filter"]:
            filter_fields=args["filter"].split(":")
            search["query"]={"bool":{"must":[{"query_string":{"query":args["q"]}},{"match":{filter_fields[0]+".keyword":":".join(filter_fields[1:])}}]}}
        else:
            search["query"]={"match_all":{}}
        if args["sort"] and ":" in args["sort"] and ( "asc" in args["sort"] or "desc" in args["sort"] ):
            sort_fields=args["sort"].split(":")
            search["sort"]=[{sort_fields[0]+".keyword":sort_fields[1]}]
        #    print(json.dumps(search,indent=4))
        searchindex=get_indices()
        if len(searchindex)>2:
            searchindex=','.join(searchindex[:-1])
        else:
            searchindex=searchindex[0]
        res=es.search(index=searchindex,body=search,size=args["size"],from_=args["from"])
        if "hits" in res and "hits" in res["hits"]:
            for hit in res["hits"]["hits"]:
                retarray.append(hit.get("_source"))
        return output.parse(retarray,args.get("format"),"",request)

if config.get("show_aut"):
    @api.route('/<any({}):authority_provider>/<string:id>'.format(str(list(authorities.keys()))),methods=['GET'])
    @api.param('authority_provider','The name of the authority-provider to access. Allowed Values: {}.'.format(str(list(authorities.keys()))))
    @api.param('id','The ID-String of the authority-identifier to access. Possible Values (examples): 208922695, 118695940, 20474817, Q1585819')
    class AutSearch(Resource):
        parser = reqparse.RequestParser()
        parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
        parser.add_argument('size',type=int,help="Configure the maxmimum amount of hits to be returned",location="args",default=100)
        parser.add_argument('from',type=int,help="Configure the offset from the frist result you want to fetch",location="args",default=0)

        @api.response(200,'Success')
        @api.response(404,'Record(s) not found')
        @api.expect(parser)
        @api.doc('get record by authority-id')
        def get(self,authority_provider,id):
            """
            search for an given ID of a given authority-provider
            """
            print(type(self).__name__)
            retarray=[]
            args=self.parser.parse_args()
            name=""
            ending=""
            if "." in id:
                dot_fields=id.split(".")
                name=dot_fields[0]
                ending=dot_fields[1]
            else:
                name=id
                ending=""
            if not authority_provider in authorities:
                abort(404)
            search={"_source":{"excludes":excludes},"query":{"query_string" : {"query":"sameAs.keyword:\""+authorities.get(authority_provider)+name+"\""}}}
            res=es.search(index=','.join(get_indices()),body=search,size=args.get("size"),from_=args.get("from"))
            if "hits" in res and "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    retarray.append(hit.get("_source"))
            return output.parse(retarray,args.get("format"),ending,request)

if config.get("show_aut"):
    @api.route('/<any({aut}):authority_provider>/<any({ent}):entity_type>/<string:id>'.format(aut=str(list(authorities.keys())),ent=get_indices()),methods=['GET'])
    @api.param('authority_provider','The name of the authority-provider to access. Allowed Values: {}.'.format(str(list(authorities.keys()))))
    @api.param('entity_type','The name of the entity-index to access. Allowed Values: {}.'.format(get_indices()))
    @api.param('id','The ID-String of the authority-identifier to access. Possible Values (examples): 208922695, 118695940, 20474817, Q1585819')
    class AutEntSearch(Resource):
        parser = reqparse.RequestParser()
        parser.add_argument('format',type=str,help="set the Content-Type over this Query-Parameter. Allowed: nt, rdf, ttl, nq, jsonl, json",location="args")
        parser.add_argument('size',type=int,help="Configure the maxmimum amount of hits to be returned",location="args",default=100)
        parser.add_argument('from',type=int,help="Configure the offset from the frist result you want to fetch",location="args",default=0)

        @api.response(200,'Success')
        @api.response(404,'Record(s) not found')
        @api.expect(parser)
        @api.doc('get record by authority-id and entity-id')
        def get(self,authority_provider,entity_type,id):
            """
            search for an given ID of a given authority-provider on a given entity-index
            """
            print(type(self).__name__)
            retarray=[]
            args=self.parser.parse_args()
            name=""
            ending=""
            if "." in id:
                dot_fields=id.split(".")
                name=dot_fields[0]
                ending=dot_fields[1]
            else:
                name=id
                ending=""
            if not authority_provider in authorities or entity_type not in get_indices():
                abort(404)
            search={"_source":{"excludes":excludes},"query":{"query_string" : {"query":"sameAs.keyword:\""+authorities.get(authority_provider)+name+"\""}}}
            res=es.search(index=entity_type,body=search,size=args.get("size"),from_=args.get("from"))
            if "hits" in res and "hits" in res["hits"]:
                for hit in res["hits"]["hits"]:
                    retarray.append(hit.get("_source"))
            return output.parse(retarray,args.get("format"),ending,request)

if config.get("show_source"):
    @api.route('/source/<string:source_index>/<string:id>'.format(ent=str(indices)),methods=['GET'])
    @api.param('source_index','The name of the source-index to access the source-data. Allowed Values: kxp-de14, swb-aut')
    @api.param('id','The ID-String of the entity to access.')
    class GetSourceData(Resource):

        @api.response(200,'Success')
        @api.response(404,'Record(s) not found')
        @api.doc('get source record by entity and entity-id')
        def get(self,source_index,id):
            print(type(self).__name__)
            if source_index=="kxp-de14":
                res=bibsource_es.get(index=source_index,doc_type="mrc",id=id)
                if "_source" in res:
                    return jsonify(res["_source"])
                else:
                    abort(404)
            elif source_index=="swb-aut":
                res=es.get(index=source_index,doc_type="mrc",id=id)
                if "_source" in res:
                    return jsonify(res["_source"])
                else:
                    abort(404)


if __name__ == '__main__':
    import socket
    if socket.gethostname() == "sdvlodapi":
        app.run(host="sdvlodapi",port=80,debug=True)
    else:
        app.run(host="localhost",port=8080,debug=True)
