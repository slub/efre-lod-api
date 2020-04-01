import sys
from flask import Flask
from flask import render_template
from flask_cors import CORS
from flask_restx import Api

from lod_api import CONFIG
from lod_api.swagger.ui import swagger_ui


app = Flask(__name__)
CORS(app)
app.config["JSONIFY_PRETTYPRINT_REGULAR"] = False

api = Api(title=CONFIG.get("apititle"),
          contact=CONFIG.get("contact"),
          contact_email=CONFIG.get("contact_email"),
          doc=CONFIG.get("frontend_url")
          )

# dynamically import different namespaces according to the
# configuration 'provide_endpoints' in the configuration
# file.
if CONFIG.get("provide_endpoints"):
    for namespace in CONFIG.get("provide_endpoints"):
        ns_api = getattr(__import__(namespace, fromlist=["api"]), "api")
        api.add_namespace(ns_api)
else:
    print("ERROR: Please provide at least one valid endpoint for the api "
          "to use, set via \"provide_endpoints\" in your main configuration "
          "file.")
    exit(1)
    

if CONFIG.get("frontend_template"):
    app.register_blueprint(swagger_ui)
    @api.documentation
    def render_swagger_page():
        return(render_template(CONFIG.get("frontend_template"),
                               title=CONFIG.get("frontend_page_title"),
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
