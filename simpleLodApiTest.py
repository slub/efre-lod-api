#!/usr/bin/python3
from requests import get
from random import randint
from json import load

with open("apiconfig.json", "r") as inp:
    config = load(inp)

Pass = True
retrieveSet = set()
for index in config["indices"]:
    url = config["base"]+"/"+config["indices"][index]["index"]+"/search?size_arg=1000"
    r = get(url)
    if r.ok and isinstance(r.json(), list):
        for elem in r.json():
            uri = elem.get("@id").split("/")
            uri[-2] = config["indices"][index]["index"]
            retrieveSet.add("/".join(uri))
    else:
        print("TEST FAILED with {}".format(url))
        Pass = False
for url in retrieveSet:
    r = get(url)
    if r.ok and isinstance(r.json(), list):
        continue
    else:
        print("TEST FAILED with {}".format(url))
        Pass = False
url1 = None
url2 = None
while True:             # we wanna test a random record, but not a resources record, because its sometimes impossible to transform them to other LOD-encodings since stupid RVK
    url1 = list(retrieveSet)[randint(0, len(retrieveSet))]
    if not "resources" in url1:
        break
while True:
    url2 = list(retrieveSet)[randint(0, len(retrieveSet))]
    if not "resources" in url2:
        break
for k, v in {"nt": "application/n-triples", "rdf": "application/rdf+xml", "ttl": 'text/turtle', "nq": 'application/n-quads', "json": 'application/json'}.items():
    if url1.startswith("http://"):
        url1 = url1.replace("http", "https")
    if url2.startswith("http://"):
        url2 = url2.replace("http", "https")
    req = get(url1+"."+k)
    if not req.ok or v not in req.headers["Content-Type"]:
        print("TEST FAILED: ", url1, v, req.headers)
        Pass = False
    req = get(url2, headers={"content-type": v})
    if not req.ok or v not in req.headers["Content-Type"]:
        print("TEST FAILED: ", url2, v, req.headers)
        Pass = False
    else:
        print(req.status_code)
if Pass:
    print("TEST PASSED")
    exit(0)
else:
    exit(-1)
