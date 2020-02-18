#!/bin/bash

set -e

host=http://${2}:9200

while [ ! `curl $host 2>/dev/null` ]; do 
    sleep 1
    echo "waiting for elasticsearch to start…"
done

for i in `ls ${1}/*`; do
    # get the file in the directory $i as index
    index=`echo ${i} | rev |cut -d "/" -f1 | rev`
    string=${host}/${index}
    curl -XDELETE ${string}; echo ""
    if [ "${index}" != "swb-aut" ] && [ "${index}" != "kxp-de14" ]; then
        curl -XPUT ${string} -d '{"mappings":{"schemaorg":{"date_detection":false}}}' \
                             -H "Content-Type: application/json"; echo ""
        id="identifier"
        doctype="schemaorg"
    else
        curl -XPUT ${string} -d '{"mappings":{"mrc":{"date_detection":false}}}' \
                             -H "Content-Type: application/json"; echo ""
        # use MARC field 001 as index
        id="001"
        doctype="mrc"
    fi
    cat "${i}" | esbulk -server ${host} -type ${doctype} -index ${index} -id ${id} -w 1 -verbose

    # # alternative to use the bulk-API from elasticsearch
    # es_bulk_input=""
    # while read -r line; do
    #     es_id=`echo "${line}" | jq -r .$id`
    #     if [ "$es_id" == "" ]; then
    #         echo "id not found: ${line}"
    #     fi
    #     es_bulk_input+='{ "index": { "_index": "'"${index}"'", "_type": "'"${doctype}"'", "_id": "'"${es_id}"'"}}'"\n${line}\n"
    # done < "${i}"
    # echo -e "$es_bulk_input" > tmp.ndjson
    # curl -XPOST "${host}/_bulk" \
    #      -H "Content-Type: application/x-ndjson"  \
    #      --data-binary "@tmp.ndjson" 2>/dev/null
done
