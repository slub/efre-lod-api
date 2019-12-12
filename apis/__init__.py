# use `run_via_bjoern.py` to run this in production behind an nginx
# use `python3 flask_api.py` to run during debugging
#
# inspired by lobid.org
#
#

import sys
import json
from flask_restplus import Api

from apis.response import Response

with open("apiconfig.json") as data_file:
    apiconfig = json.load(data_file)
    api = Api(title=apiconfig.get("apititle"),
              default=apiconfig.get("apiname"),
              default_label=apiconfig.get("default_label"),
              default_mediatype=apiconfig.get("default_mediatype"),
              contact=apiconfig.get("contact"),
              contact_email=apiconfig.get("contact_email"),
              doc='/doc/api/')


output = Response(api)


# Import must be here in order for cyclic imports to work:
#    `apis.reconcile.response` imports `output` from here
# flake8: noqa
from apis.reconcile.response import data_to_preview
# extend Output class by OpenRefine's preview functionality
# register new output function to the file extension "preview"
output.add("preview", data_to_preview, mediatype="openrefine/preview")


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
        return {'message': "Internal Server Error: "+traceback_details['message']}, 500
    else:
        return {'message': 'Internal Server Error'}, 500
