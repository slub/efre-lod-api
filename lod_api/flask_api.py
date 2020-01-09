from flask import Flask
from flask import render_template
from flask_cors import CORS

from lod_api import CONFIG
from lod_api.swagger.ui import swagger_ui

from lod_api.apis import api
from lod_api.apis.source import api as ns_source
from lod_api.apis.authority_provider import api as ns_authority
from lod_api.apis.search_and_access import api as ns_search
from lod_api.apis.reconcile import api as ns_reconcile

app = Flask(__name__)
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


@api.errorhandler(Exception)
def generic_exception_handler(e: Exception):
    def get_type_or_class_name(var) -> str:
        if type(var).__name__ == 'type':
            return var.__name__
        else:
            return type(var).__name__

    exc_type, exc_value, exc_traceback = sys.exc_info()

    if exc_traceback:
        traceback_details = {
            'filename': exc_traceback.tb_frame.f_code.co_filename,
            'lineno': exc_traceback.tb_lineno,
            'name': exc_traceback.tb_frame.f_code.co_name,
            'type': get_type_or_class_name(exc_type),
            'message': str(exc_value),
        }
        return {'message': "Internal Server Error: " + traceback_details['message']}, 500
    else:
        return {'message': 'Internal Server Error'}, 500


api.init_app(app)


def run_app():
    app.run(host=CONFIG.get("debug_host"),
            port=CONFIG.get("debug_port"),
            debug=True)
    return app


if __name__ == "__main__":
    run_app()
