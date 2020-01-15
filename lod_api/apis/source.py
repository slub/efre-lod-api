import requests
import flask
from flask_restplus import Namespace

from lod_api.tools.resource import LodResource
from lod_api import CONFIG

api = Namespace("source", path="/",
                description="Source data access operation")

# flaskREST+ BUG, which ignores the last element in <any([…])> list
#     [see](https://github.com/noirbizarre/flask-restplus/issues/695)
# quickfix: add whitespace string as element
@api.route('/source/<any({ent}):source_index>/<string:id>'.
           format(ent=CONFIG.get("sources_list") + [" "]), methods=['GET'])
@api.param('source_index',
           'The name of the source-index to access the source-data.'
           'Allowed Values: {ent}'.format(ent=CONFIG.get("sources_list")))
@api.param('id', 'The ID-String of the entity to access.')
class GetSourceData(LodResource):
    source_indices = CONFIG.get("source_indices")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.doc('get source record by entity and entity-id')
    def get(self, source_index, id):
        print(type(self).__name__)
        if self.source_indices.get(source_index):
            url = "{}{}".format(self.source_indices[source_index], id)
            res = requests.get(url)
            if res.ok and "_source" in res.json():
                return flask.jsonify(res.json()["_source"])
            else:
                flask.abort(404)
