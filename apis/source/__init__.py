import requests
import flask
from flask_restplus import Resource
from flask_restplus import Namespace
from apis.helper_functions import get_indices
from apis.helper_functions import load_config

api = Namespace("source", path="/", description="Source data access operation")


@api.route('/<string:source_index>/<string:id>'.format(ent=str(get_indices())), methods=['GET'])
@api.param('source_index', 'The name of the source-index to access the source-data. Allowed Values: kxp-de14, swb-aut')
@api.param('id', 'The ID-String of the entity to access.')
class GetSourceData(Resource):
    source_indices = load_config("apiconfig.json", "source_indices")

    @api.response(200, 'Success')
    @api.response(404, 'Record(s) not found')
    @api.doc('get source record by entity and entity-id')
    def get(self, source_index, id):
        print(type(self).__name__)
        import IPython
        IPython.embed()
        if self.source_indices.get(source_index):
            url = "{}{}".format(self.source_indices[source_index], id)
            res = requests.get(url)
            if res.ok and "_source" in res.json():
                return flask.jsonify(res.json()["_source"])
            else:
                flask.abort(404)
