import flask
from flask_restx import Namespace
from flask_restx import reqparse
import elasticsearch


from lod_api.tools.resource import LodResource
from lod_api.tools.helper import ES_wrapper
from lod_api import CONFIG

api = Namespace(name="explorative search", path="/",
                description="API endpoint to be use with the explorative search webapp, see <URL>")


def translateBackendToWebapp():
    pass


@api.route('/explore/topicsearch', methods=['GET'])
class searchDoc(LodResource):
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

    es_host, es_port, excludes, indices = CONFIG.get("es_host", "es_port", "excludes", "indices")
    es = elasticsearch.Elasticsearch([{'host': es_host}], port=es_port, timeout=10)

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.expect(parser)
    @api.doc('search in Index')
    def get(self):
        """
        perform a simple serach on the topics index
        """
        print(type(self).__name__)
        retdata = []
        args = self.parser.parse_args()
        # TOOD: multi_match types best_fields, most_fields, cross_fields, phrase, phrase_prefix, bool_prefix
        # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
        # multi_match:
        #   query,
        #   fields: ['*'],
        #
        # type: 'most_fields'
        # }
        query = {
                'size': args.get("size"),
                '_source': self.excludes,
                'query': {
                    "simple_query_string": {
                        'query':  args.get("q"),
                        'fields': args.get("fields"),
                        'default_operator': 'and'
                        }
                    }
                }

        res = ES_wrapper.call(
                self.es,
                action="search",
                index="topics-explorativ",
                body=query,
                _source_excludes=self.excludes
            )

        if res["hits"] and res["hits"]["hits"]:
            for r in  res["hits"]["hits"]:
                retdata.append({
                    "id":            r["_source"]["@id"].replace("topics", "topics"),
                    "score":         r["_score"],
                    "name":          r["_source"]["preferredName"],
                    "alternateName": r["_source"].get("alternateName", []),
                    "description":   r["_source"].get("description", ""),
                    "additionalTypes": [],                # fill later on
                    }
                )
                if r["_source"].get("additionalType"):
                    # process all additionalType entries individually
                    for adtype in r["_source"]["additionalType"]:
                        retdata[-1]["additionalTypes"].append({
                            "id":          adtype["@id"].replace("topics", "topics"),
                            "name":        adtype["name"],
                            "description": adtype["description"]
                        }
                    )



        return self.response.parse(retdata, "json", "", flask.request)

