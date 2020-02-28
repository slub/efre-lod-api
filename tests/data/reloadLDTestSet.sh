#!/bin/bash

set -e

host=http://${2}:9200
tmp_folder="/tmp/lod"
mkdir -p $tmp_folder

generate_esbulk (){
    infile="$1"
    index="$2"
    doctype="$3"
    id="$4"

    es_bulk_input=""
    while read -r line; do
        es_id=`echo "${line}" | jq -r ".[\"$id\"]"`
        if [ "$es_id" == "" ]; then
            echo "id not found: ${line}"
            exit 1
        fi
        es_bulk_input+='{ "index": { "_index": "'"${index}"'", "_type": "'"${doctype}"'", "_id": "'"${es_id}"'"}}'"\n${line}\n"
    done < "${infile}"
    echo -e "$es_bulk_input" > ${tmp_folder}/${index}.jsonl
}

echo -n "waiting for elasticsearch to start"
while ! curl $host 2>/dev/null ; do 
    sleep 1
    echo -n "…"
done
echo ""

for i in `ls ${1}/*`; do
    # get the file in the directory $i as index
    index=`echo ${i} | rev |cut -d "/" -f1 | rev`
    string=${host}/${index}
    # curl -XDELETE ${string} 2>/dev/null || true ; echo ""
    if [ "${index}" != "swb-aut" ] && [ "${index}" != "kxp-de14" ]; then
        curl -XPUT ${string} -d '{"mappings":{"schemaorg":{"date_detection":false}}}' \
                             -H "Content-Type: application/json" 2>/dev/null; echo ""
        id="identifier"
        doctype="schemaorg"
    else
        curl -XPUT ${string} -d '{"mappings":{"mrc":{"date_detection":false}}}' \
                             -H "Content-Type: application/json" 2>/dev/null; echo ""
        # use MARC field 001 as index
        id="001"
        doctype="mrc"
    fi
    if which esbulk >/dev/null 2>&1; then
        # prefer esbulk for data injection to elasticsearch
        cat "${i}" | esbulk -server ${host} -type ${doctype} -index ${index} -id ${id} -w 1 -verbose
    else
        # alternative to use the bulk-API from elasticsearch
        if [ ! -f ${tmp_folder}/${index}.jsonl ]; then
            generate_esbulk $i $index $doctype $id
        fi
        curl -XPOST "${host}/_bulk" \
             -H "Content-Type: application/x-ndjson"  \
             --data-binary "@${tmp_folder}/${index}.jsonl" >/dev/null 2>&1
    fi

done
