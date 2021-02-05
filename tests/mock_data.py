import os
import lod_api
import difflib
import json
from collections import OrderedDict


class MockDataHandler():
    def __init__(self):
        # define path for test data
        self.path = os.path.join(lod_api.__path__[0], "../../tests/data/mockout/")
        print(self.path)

    def normalize_data(self, data, format):
        """ normalize given data with a given format in order to compare them with data dumps.
        """
        if format == "nq":
            print("sort nq file")
            lines = sorted(data.split("\n"))
            data_out = "\n".join(lines)
        else:
            data_out = data
        # if format == "json":
        #     data_json = json.loads(data, object_pairs_hook=orderedDict)
        #     data_out = json.dumps(data_json)
        # elif format == "jsonl":
        #     lines = []
        #     for js in data.split("\n"):
        #         if js:
        #             print(js)
        #             data_json = json.loads(js, object_pairs_hook=orderedDict)
        #             lines.append(json.dumps(data_json))
        #     data_out = "\n".join(lines)
        return data_out

    def _sanitize_fname(self, fname, extension=".dat"):
        fname = fname.replace("/", "-")
        return fname + extension

    def write(self, fname, data, format=None):
        fname = self._sanitize_fname(fname)

        with open(os.path.join(self.path, fname), "w+") as outfile:
            outfile.write(self.normalize_data(data, format))

    def compare(self, fname, data, format=None):
        fname = self._sanitize_fname(fname)
        output_data = self.normalize_data(data, format)

        with open(os.path.join(self.path, fname), "r") as infile:
            compdata = infile.read()

        diff = difflib.unified_diff(output_data, compdata,
                fromfile="API-data", tofile=fname)
        print("".join(diff))
        assert(output_data == compdata)
