# docker run -it --rm ES /bin/bash
# FROM docker.elastic.co/elasticsearch/elasticsearch:6.8.2
FROM centos:latest

ARG esversion=6.8.2

# elasticsearch dependencies
# centos:
RUN yum install -y java-1.8.0-openjdk-headless wget curl bzip2 jq
RUN useradd --create-home --shell  /bin/bash metadata
USER metadata

# download and install elasticsearch
WORKDIR /home/metadata
RUN wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${esversion}.tar.gz 2>/dev/null &&\
    wget https://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${esversion}.tar.gz.sha512 2>/dev/null &&\
    sha512sum -c elasticsearch-${esversion}.tar.gz.sha512 &&\
    tar -xzf elasticsearch-${esversion}.tar.gz &&\
    mv elasticsearch-${esversion} /home/metadata/elasticsearch &&\
    rm elasticsearch-${esversion}.tar.gz

#
RUN mkdir -p /tmp/lod/
COPY data/LDTestSet.tar.bz2 /tmp/lod/
COPY data/reloadLDTestSet.sh /tmp/lod/
RUN nohup bash -c "/home/metadata/elasticsearch/bin/elasticsearch -d -p es.pid" && \
    cd /tmp/lod &&\
    tar -xjf LDTestSet.tar.bz2 && \
    bash reloadLDTestSet.sh ldj localhost && \
    kill $(cat /home/metadata/elasticsearch/es.pid) &&\
    rm -r /tmp/lod

USER root

EXPOSE 9200
EXPOSE 9300

USER metadata
WORKDIR /home/metadata
CMD ["bash", "/home/metadata/elasticsearch/bin/elasticsearch", "-E", "network.host=0.0.0.0"]
