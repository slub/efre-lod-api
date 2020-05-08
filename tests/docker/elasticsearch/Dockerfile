# docker run -it --rm ES /bin/bash
# FROM docker.elastic.co/elasticsearch/elasticsearch:6.8.2
FROM centos:latest

# defined as build-arg
ARG esmaj_version

# elasticsearch dependencies
# centos:
RUN yum install -y java-1.8.0-openjdk-headless wget curl bzip2 jq gawk
RUN useradd --create-home --shell  /bin/bash metadata
WORKDIR /home/metadata

# download and install elasticsearch
COPY docker/elasticsearch/install_elasticsearch.sh /home/metadata/
RUN ["chmod", "+x", "/home/metadata/install_elasticsearch.sh"]
USER metadata
RUN /home/metadata/install_elasticsearch.sh $esmaj_version

RUN mkdir -p /tmp/lod/
COPY data/LDTestSet.tar.bz2 /tmp/lod/
COPY data/reloadLDTestSet.sh /tmp/lod/
COPY docker/elasticsearch/elasticsearch.yml /home/metadata/elasticsearch/config/
RUN nohup bash -c "/home/metadata/elasticsearch/bin/elasticsearch -d -p es.pid" && \
    cd /tmp/lod &&\
    tar -xjf LDTestSet.tar.bz2 && \
    bash reloadLDTestSet.sh ldj localhost && \
    kill $(cat /home/metadata/elasticsearch/es.pid) &&\
    rm -r /tmp/lod

EXPOSE 9200
EXPOSE 9300

USER metadata
WORKDIR /home/metadata
CMD ["bash", "/home/metadata/elasticsearch/bin/elasticsearch"]
