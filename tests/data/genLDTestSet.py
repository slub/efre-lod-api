#! /usr/bin/env python3

import elasticsearch
import json
import re
import yaml
import argparse
from pathlib import Path
from urllib.parse import urlparse


def extract_URIs(iterator, pref_pattern, field=None):
    """ Takes iterator of dicts  and extracts URI matching specified prefix
        pattern. Returns a set containing all URIs.
    """
    uris = set()
    for i in iterator:
        if field and field in i:
            i = i[field]
        # extract URIs
        # add all uris to the set using update()
        # Serialize dictonary into json string
        ser = json.dumps(i)

        # non-greedy match until the next found \"
        pattern = pref_pattern + '(.*?)\"'
        uris.update(re.findall(pattern, ser))
    return uris


def count_entities(uri_set):
    """ count entities in set 'uri_set' by ommitting everything after the
        last "/". Returns dict with names and counts
    """
    ent_count = dict()                                     # entity counter
    for resource in uri_set:
        # remove id from resource to get entity
        entity = "/".join(resource.split("/")[:-1])
        if type(ent_count.get(entity)) == int:
            ent_count[entity] += 1
        else:
            ent_count[entity] = 1
    return ent_count


def bin_entities(uri_set, delimiter="/", splitpos=-1):
    """ Takes iteratable elemts and splits them according to the position
        (splitpos) of the delimiter. The first part is used as a key,
        whereas the second appended to a list connected to the former key.

        return: dict {key1: [id11, id12, id13, …], key2: […}}
    """
    ent_dict = dict()
    for res in uri_set:
        # split entity up to splitpos using delimiter
        entity = delimiter.join(res.split(delimiter)[:splitpos])
        # id_ is the remainder
        id_ = delimiter.join(res.split(delimiter)[splitpos:])

        if entity in ent_dict:
            ent_dict[entity].append(id_)
        else:
            ent_dict[entity] = [id_]
    return ent_dict


def requ_es_index(instance, ids, check=True):
    """ requests a single ES 'instance' (form parsable by urlparse) to querry
        all given ids. If 'check' is True, the results are checked again if all
        ids have been found correctly.

        instance: http://localhost:9200/index/doc_type

        returns: list with ES results
    """
    if len(ids) == 0:
        return []
    o = urlparse(instance)
    es = elasticsearch.Elasticsearch([{"host": o.hostname}], port=o.port,
                                     timeout=20)
    index, doc_type = o.path.split("/")[1:]
    res = es.mget(index=index, doc_type=doc_type, body={"ids": ids})

    # check returned docs for existance
    if check:
        for i, doc in enumerate(res["docs"]):
            if not doc["found"]:
                print("{}/{} not found".format(instance, ids[i]))

    return res["docs"]


def requ_es_compl(es_instance, indices, ids,
                  id_path="identifier.keyword",
                  uri_source_path="@id"):
    """ Requests all indices from es_instance for the requested ids.

        returns: List of elasticsearch results
    """
    o = urlparse(es_instance)
    es = elasticsearch.Elasticsearch([{"host": o.hostname}], port=o.port,
                                     timeout=20)
    doc_list = []
    for id in ids:
        query = {
                "query": {
                    "query_string": {
                        "query": "{path}:{id}".format(path=id_path, id=id)
                        }
                    }
                }
        res = es.search(index=",".join(indices), body=query)
        if int(res["hits"]["total"]) == 1:
            doc_list.append(res["hits"]["hits"][0]["_source"][uri_source_path])
        elif int(res["hits"]["total"]) > 1:
            print("something went wrong here: found more than one document "
                  "with id \"{id}\" in \"{path}\"".format(id=id, path=id_path))
    return doc_list


def aggregate_disj_ancestors(prim_coll, mapping, link_identifier):
    """ Use a collection of primary URIs and request their associated
        secondary URIs.

        returns: a set with secondary URIs not contained in the primary
                 collection
    """
    ent_dict = bin_entities(prim_coll)
    sec_coll = set()
    for ent in ent_dict.keys():
        ids = ent_dict[ent]
        try:
            instance = mapping[ent]
            secondary_docs = requ_es_index(instance, ids)
        except KeyError:
            # URI cannot be directly resolved with a elasticearch index
            # therefore: search over all indices of all elasticsearch instances

            # parse all es instances with corresponding indices
            inst_indices = bin_entities(mapping.values(), splitpos=-2)

            secondary_docs = []
            for instance, indices in inst_indices.items():
                # remove doctype from indices
                index_list = [x.split("/")[0] for x in indices]
                secondary_docs += requ_es_compl(instance, index_list, ids)

        sec_coll = sec_coll.union(extract_URIs(secondary_docs,
                                               link_identifier,
                                               field="_source"))
    return sec_coll.difference(prim_coll)


def get_initial_coll(es_index, link_prefix, size=50, seed=0, requirements={}):
    """ Request 'size' of data from given es-index in random order
        (seed of random request can be changed). Optionally
        requirements can be defined to be met. See below.

        link_prefix: String that defines the beginning of a URI from which the
          links are generated. This prefix is furthermore removed form the
          resulting link set.

        requirements:
          dict of the form {"key1": int1, "key2": int2, …}
          requiremts are fullfilled, if the key is at least int-times in the
          resulting set of the first request.

        returns: set of links
    """

    o = urlparse(es_index)
    index, doc_type = o.path.split("/")[1: 3]

    es = elasticsearch.Elasticsearch(
            [{"host": o.hostname}],
            port=o.port,
            timeout=20,
            max_retries=10,
            retry_on_timeout=True)

    # query randomly documents until specific number of /works and /events
    # are reached. increase seed by one if condition not met
    requirements_met = False
    while not requirements_met:
        query = {"size": size,
                 "query": {
                     "function_score": {
                         "random_score": {
                             "seed": seed
                             }
                         }
                     }
                 }
        res = es.search(index=index, doc_type=doc_type, body=query)

        # collection set of all linked resources
        res_collection = set()

        # generate a set of all linked resources
        res_collection = extract_URIs(res["hits"]["hits"],
                                      link_prefix,
                                      field="_source")
        stat = count_entities(res_collection)

        requirements_met = True
        for rq_ind, rq_num in requirements.items():
            if stat.get(rq_ind):
                if stat.get(rq_ind) < rq_num:
                    print("{} too less ({})".format(rq_ind, rq_num))
                    requirements_met = False
            else:
                print("{} none".format(rq_ind))
                requirements_met = False

        print("{} - works: {}, events: {}".format(seed,
                stat.get("/works"), stat.get("/events")))
        seed += 1
    return res_collection


def enrich_linked_data(collection, es_lookup, link_identifier):
    """ Request URIs linked from the given collection and resolve the
        requested URIs furthermore to find connections of second order.
        Repeating this process until all internal links are resolved.

        es_lookup: dict. Lookup table (endpoint name → elasticsearch
          instance with the containing index)

        returns: set. collection of all enriched URIs
    """
    # make a copy of collection in order not to mutate it
    res_collection = collection

    prim_coll = res_collection
    print("new/total\t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
        "resour", "pers", "geo", "organi", "topics", "works", "events"))
    while len(prim_coll) > 0:
        stat = count_entities(res_collection)
        print("{}/{} \t{}\t{}\t{}\t{}\t{}\t{}\t{}".format(
            len(prim_coll), len(res_collection),
            stat.get("/resources"), stat.get("/persons"),
            stat.get("/geo"), stat.get("/organizations"),
            stat.get("/topics"), stat.get("/works"),
            stat.get("/events")))
        sec_coll = aggregate_disj_ancestors(prim_coll, es_lookup,
                                            link_identifier)
        prim_coll = sec_coll.difference(res_collection)
        res_collection = res_collection.union(sec_coll)

    return res_collection


def parse_options():
    p = argparse.ArgumentParser()
    p.add_argument("-c", "--config", required=True,
            help="lod-api config file")
    p.add_argument("-s", "--sample-size", type=int, default=50,
            help="initial sample size to request")
    p.add_argument("-o", "--outdir", default="ldj_out")
    p.add_argument("-i", "--input",
            help="older output directory to get the data from there in "
                 "order to update the test-dataset.")
    return p


def gen_lookup(file):
    """ Generating the lookup table between api-endpoints and elasticsearch
        instances from the configuration of the lod-api.

        lookup table is of the form:
            {"/api/endpoint": "http://elasticsearchhost:port/index/doctype",
             "/resource": "http://elasticsearchhost:9200/resource/data",
             "/works": …,
            }
        returns: dict.
    """
    es_lookup = dict()

    # read complete api configuration
    with open(file, 'r') as instream:
        config = yaml.safe_load(instream)

    # generate lookup for source indices
    for source, instance in config["source_indices"].items():
        key = "/source/{}".format(source)
        # remove trailing slash from index-URL
        es_lookup[key] = instance.rstrip("/")

    # generate remaining index lookups
    es_address = "http://{h}:{p}".format(h=config["es_host"],
                                          p=config["es_port"])
    for ctx in config["indices"]:
        doctype = config["indices"][ctx].get("type")
        index = config["indices"][ctx].get("index")
        es_lookup["/" + index] = "{h}/{i}/{t}".format(
                h=es_address, i=index, t=doctype)

    return es_lookup


def main():
    # configuration stuff
    p = parse_options()
    args = p.parse_args()
    es_lookup = gen_lookup(args.config)

    link_prefix = "https://data.slub-dresden.de"
    target_replacement = "http://localhost:8080"  # → replaces link_prefix
                                                  #   in the output data
    sample_size = int(args.sample_size)           # → initial sample size
    require = {"/works": 1, "/events": 1}         # → requiremnts on the number
                                                  #   of documents connected
                                                  #   initially
    mesh_center = "/resources"                    # → center of linked data

    if args.input:
        # take initial URIs from given dataset, reading the @id from
        # line-delimited json file
        input_file = args.input + "/" + mesh_center.split("/")[-1]
        init_coll = set()
        with open(input_file, 'r') as file:
            for line in file.readlines():
                dataset = json.loads(line)
                url = urlparse(dataset["@id"])
                init_coll.add(url.path)
    else:
        # generate random set with linked data originating from mesh_center
        init_coll = get_initial_coll(es_lookup[mesh_center], link_prefix,
                                     size=sample_size, seed=15,
                                     requirements=require)

    coll = enrich_linked_data(init_coll, es_lookup, link_prefix)

    # create output directory if non-existant
    Path(args.outdir).mkdir(parents=True, exist_ok=True)

    # replace data.slub-dresden address with ES address
    ids_by_ent = bin_entities(coll)

    for ent in ids_by_ent.keys():
        if not es_lookup.get(ent):
            # entity not found in lookup-table - however: documents have been
            # resolved by aggregate_disj_ancestors function. Thus,
            # all ids are present in one of the other entities
            continue
        instance = es_lookup[ent]
        ids = ids_by_ent[ent]
        docs = requ_es_index(instance, ids, check=False)

        filecontent = ""
        for doc in docs:
            if not doc["found"]:
                continue

            flatdoc = json.dumps(doc["_source"])
            flatdoc = flatdoc.replace(link_prefix, target_replacement)
            filecontent += flatdoc + "\n"
        # write filecontent in directory 'ldj' with entities as filename but
        # using only the last part separated by "/"
        with open(args.outdir + "/" + ent.split("/")[-1], 'w+') as file:
            file.write(filecontent)


if __name__ == "__main__":
    main()
