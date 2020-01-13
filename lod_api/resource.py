from flask_restplus import Resource
from lod_api.apis.response import Response
from lod_api.apis.reconcile_response import data_to_preview 


class LodResource(Resource):
    """ Extents flastREST+' Resource class to provide
        different response types in the endpoint routines
        triggered by flaskREST+.
    """
    def __init__(self, api=None, *args, **kwargs):
        # initialyze output support
        self.response = Response(api)

        # TODO: Write a generic loader for Response extensions
        # extend response class by OpenRefine's preview functionality
        self.response.add("preview", data_to_preview)

        super().__init__(api=api, *args, **kwargs)

