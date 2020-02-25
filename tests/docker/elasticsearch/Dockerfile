# docker run -it --rm ES /bin/bash
# FROM docker.elastic.co/elasticsearch/elasticsearch:6.8.2
FROM centos:latest

ARG esversion=6.8.2
ARG api_branch=master

# elasticsearch dependencies
# centos:
RUN yum install -y java-1.8.0-openjdk-headless wget curl # bzip2 go git
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

# # download and install esbulk
# RUN go get github.com/miku/esbulk/cmd/esbulk
#
# COPY LDTestSet.tar.bz2 /home/metadata/
# COPY reloadLDTestSet.sh /home/metadata/
# RUN nohup bash -c "/home/metadata/elasticsearch/bin/elasticsearch -d -p es.pid" && \
#     tar -xjf LDTestSet.tar.bz2 && \
#     while [ ! `curl localhost:9200 2>/dev/null` ]; do sleep 1; echo "waiting for elasticsearch to start…"; done && \
#     bash reloadLDTestSet.sh ldj localhost 2>/dev/null && \
#     kill $(cat elasticsearch/es.pid)
#
# # clearup
# USER root
# RUN yum remove -y go git

EXPOSE 9200
EXPOSE 9300

USER metadata
WORKDIR /home/metadata
CMD ["bash", "/home/metadata/elasticsearch/bin/elasticsearch", "-E", "network.host=0.0.0.0"]
