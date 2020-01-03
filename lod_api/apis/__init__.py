from flask_restplus import Api

from lod_api import CONFIG
from lod_api.apis.response import Response

api = Api(title=CONFIG.get("apititle"),
          default=CONFIG.get("apiname"),
          default_label=CONFIG.get("default_label"),
          default_mediatype=CONFIG.get("default_mediatype"),
          contact=CONFIG.get("contact"),
          contact_email=CONFIG.get("contact_email"),
          doc=CONFIG.get("doc_url")
         )


output = Response(api)


# Import must be here in order for cyclic imports to work:
#    `apis.reconcile.response` imports `output` from here
# flake8: noqa
from lod_api.apis.reconcile_response import data_to_preview
# extend Output class by OpenRefine's preview functionality
# register new output function to the file extension "preview"
output.add("preview", data_to_preview, mediatype="openrefine/preview")