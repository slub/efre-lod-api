""" Extend flaskRESTPlus' Resource class by the
    functionality of the Response class to be able
    to use transformations of the response (e.g.
    json → rdf) for every endpoint
"""
from flask_restplus import Resource
from .response import Response
from lod_api.apis.reconcile import data_to_preview


class LodResource(Resource):
    """ Extents flastREST+' Resource class to provide
        different response types in the endpoint routines
        triggered by flaskREST+.
    """

    def __init__(self, api=None, *args, **kwargs):
        """ adds OpenRefine's preview functionality to the Response class."""
        # initialyze output support
        self.response = Response(api)

        # TODO: Write a generic loader for Response extensions
        # extend response class by OpenRefine's preview functionality
        self.response.add("preview", data_to_preview)

        super().__init__(api=api, *args, **kwargs)
