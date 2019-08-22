#!/usr/bin/python3
import json
import requests

with open("apiconfig.json","r") as inp:
    config = json.load(inp)

Pass=True
retrieveSet=set()
for index in config["indices"]:
    url=config["base"]+"/"+config["indices"][index]["index"]+"/search?size_arg=1000"
    r=requests.get(url)
    if r.ok and isinstance(r.json(),list):
        for elem in r.json():
            retrieveSet.add(elem.get("@id"))
    else:
        print("TEST FAILED with {}".format(url))
        Pass=False
for url in retrieveSet:
    r=requests.get(url)
    if r.ok and isinstance(r.json(),list):
        continue
    else:
        print("TEST FAILED with {}".format(url))
        Pass=False
if Pass:
    print("TEST PASSED")
    exit(0)
else:
    exit(-1)
