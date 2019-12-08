from flask import Flask
from flask import render_template
from flask_cors import CORS

from apis import api
from swagger.ui import swagger_ui
from apis.helper_functions import load_config

from apis.reconcile import api as ns_reconcile
from apis.source import api as ns_source
from apis.authority_provider import api as ns_authority
from apis.search_and_access import api as ns_search

app=Flask(__name__)
CORS(app)
app.register_blueprint(swagger_ui)

api.add_namespace(ns_search)
api.add_namespace(ns_authority)
api.add_namespace(ns_source)
api.add_namespace(ns_reconcile)


@api.documentation
def render_swagger_page():
    return(render_template('slub-swagger-ui.html',
           title="API - SLUB - LOD API documentation",
           specs_url=api.specs_url))

api.init_app(app)


if __name__ == '__main__':
    debug_host, debug_port = load_config("apiconfig.json","debug_host","debug_port")
    app.run(host=debug_host, port=debug_port, debug=True)
