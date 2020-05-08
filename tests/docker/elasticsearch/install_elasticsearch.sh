#!/bin/sh
#

esmaj_version=$1

# for exact version, see
# https://www.elastic.co/guide/en/elasticsearch/reference/index.html
if [ "$esmaj_version" == "6" ]; then
    # https://www.elastic.co/guide/en/elasticsearch/reference/6.8/es-release-notes.html
    esversion="6.8.8"
    esdlfile="elasticsearch-${esversion}.tar.gz"
elif [ "$esmaj_version" == "7" ]; then
    # https://www.elastic.co/guide/en/elasticsearch/reference/7.6/es-release-notes.html
    esversion="7.6.2"
    esdlfile="elasticsearch-${esversion}-linux-x86_64.tar.gz"
else
    echo "error: non-defined major version"
    exit 1
fi 

echo "${esversion}"

# download and install elasticsearch
wget https://artifacts.elastic.co/downloads/elasticsearch/${esdlfile} 2>/dev/null
wget https://artifacts.elastic.co/downloads/elasticsearch/${esdlfile}.sha512 2>/dev/null
sha512sum -c ${esdlfile}.sha512
tar -xzf ${esdlfile}
mv elasticsearch-${esversion} /home/metadata/elasticsearch
rm ${esdlfile}
rm ${esdlfile}.sha512
