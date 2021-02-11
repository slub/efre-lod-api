import json
import gzip
import rdflib
import io
import flask

# Check if global variable `api` is set from flaskRestPlus
# if not: create a dummy variable for dealing with the
# annotations
# try:
#     api
# except NameError:
#     print("api seems not to be in your namespace")
#     class mock_api():
#         def representation(self, string):
#             return lambda x: x
#     api = mock_api()


class Response:
    """ Response class that is mainly used throught Output.parse
        This class takes input data and transforms it according
        to the requested
          - file format (via file extension) OR
          - `format` GET parameter OR
          - media type in Request-Header

        In addition a compression with gzip is performed if
        in accordance with the `Accept-Encoding` header

        This class can be extended by adding a format
        in self.format together with a class method
        that processes the json data together with the
        request header.
    """

    def __init__(self, api):
        self.api = api

        self.format = {
            "nt": self.convert_data_to_nt,
            "ttl": self.convert_data_to_ttl,
            "rdf": self.convert_data_to_rdf,
            "nq": self.convert_data_to_nq,
            "jsonl": self.convert_data_to_jsonl,
            "json": self.convert_data_to_json
        }

        # mapping media type --> format
        self.mediatype = {
            "application/json": "json",
            "application/ld+json": "jsonl",
            "application/x-jsonlines": "jsonl",
            "application/n-triples": "nt",
            "application/rdf+xml": "rdf",
            "text/turtle": "ttl",
            "application/n-quads": "nq"
        }
        for mtype, frmt_mtype in self.mediatype.items():
            # one cannot overwrite the default parameter
            if frmt_mtype != "json":
                if self.api:
                    self._register(mtype, frmt_mtype)

    def _register(self, mtype, frmt_ext):
        """ add new mediatype for each format extension
            in order for flaskRESTPlus to show mimetypes in
            the swagger UI
            normally this is done via method annotation
            e.g.
              @api.representation("text/turtle")
              def convert_data_to_ttl(…):
                  …
        """
        frmt_fct = self.format[frmt_ext]
        frmt_fct = self.api.representation(mtype)(frmt_fct)

    def _encode(self, req, res):
        """ Checks if the client has defined `gzip` in its
            Accept-Encoding Header and compress the HTML
            responce accordingly
        """
        # print(req.headers.get("Accept-Encoding"))
        if (req.headers.get("Accept-Encoding")
                and "gzip" in req.headers.get("Accept-Encoding")):
            # Extends the Response object by the `Content-Encoding` header
            # and gzip the data from the Response
            gzip_buffer = io.BytesIO()
            with gzip.open(gzip_buffer, mode="wb", compresslevel=6) as gzip_file:
                gzip_file.write(res.data)
            res.data = gzip_buffer.getvalue()
            res.headers['Content-Encoding'] = 'gzip'
            res.headers['Vary'] = 'Accept-Encoding'
            res.headers['Content-Length'] = res.content_length
        return res

    def _parse_json(self, data):
        """ use RDFlib to parse json """
        g = rdflib.ConjunctiveGraph()
        for elem in data:
            g.parse(data=json.dumps(elem), format='json-ld')
        return g

    def add(self, frmt_ext, frmt_fct, mediatype=None):
        """ extends `self.format` and `self.mediatype` with additional
            Response type
        """
        # set new convert function
        setattr(self, frmt_fct.__name__, frmt_fct)

        # extend frmt_dict
        self.format[frmt_ext] = getattr(self, frmt_fct.__name__)

        if mediatype:
            self.mediatype[mediatype] = frmt_ext
            if self.api:
                self._register(mediatype, frmt_ext)

    def parse(self, data, get_format, file_ext, request):
        """ `parse` decides in which form the data should be
           transformed before it is served to the client.

           The decision is made according to the following
           set of rules:
             - If a file extension `file_ext` is set, this
               is used
             - else: If a format is set via the GET parameter
               `get_format` this is used
             - else: If the "Content-Type" of "Accept" is
               set in the Request-Header this is used
             - finally: deliver plain json
        """
        retformat = ""
        # parse request-header and fileending
        if request.headers.get("Content-Type"):
            encoding = request.headers.get("Content-Type")
        elif request.headers.get("Accept"):
            encoding = request.headers.get("Accept")
        else:
            # Fallback to json if nothing was given
            encoding = "json"

        file_ext_avail = [key for key in self.format]
        mediatype_avail = [key for key in self.mediatype]

        if file_ext and file_ext in file_ext_avail:
            retformat = file_ext
        elif not file_ext and get_format in file_ext_avail:
            retformat = get_format
        elif encoding in mediatype_avail:
            retformat = self.mediatype[encoding]
        else:
            retformat = "json"

        print(retformat)

        if not data:    # returns 404 if data not set
            flask.abort(404)

        # check out the format string for ?format= or Content-Type Headers
        try:
            return self.format[retformat](data, request)
        except KeyError:
            # return simple json object
            # return self.convert_data_to_json(data, request)
            return self._encode(request, flask.jsonify(data))

    def convert_data_to_json(self, data, request):
        return self._encode(request, flask.jsonify(data))

    def convert_data_to_nt(self, data, request):
        data_out = self._parse_json(data).serialize(format="nt").decode('utf-8')
        res = flask.Response(data_out, mimetype='application/n-triples')
        return self._encode(request, res)

    def convert_data_to_rdf(self, data, request):
        data_out = self._parse_json(data).serialize(format="application/rdf+xml").decode('utf-8')
        res = flask.Response(data_out, mimetype='application/rdf+xml')
        return self._encode(request, res)

    def convert_data_to_ttl(self, data, request):
        data_out = self._parse_json(data).serialize(format="turtle").decode('utf-8')
        res = flask.Response(data_out, mimetype='text/turtle')
        return self._encode(request, res)

    def convert_data_to_nq(self, data, request):
        data_out = self._parse_json(data).serialize(format="nquads").decode('utf-8')
        res = flask.Response(data_out, mimetype='application/n-quads')
        return self._encode(request, res)

    def convert_data_to_jsonl(self, data, request):
        data_out = ""
        if isinstance(data, list):
            for item in data:
                data_out += json.dumps(item, indent=None) + "\n"
        elif isinstance(data, dict):
            data_out += json.dumps(data, indent=None) + "\n"

        res = flask.Response(data_out, mimetype='application/x-jsonlines')
        return self._encode(request, res)
