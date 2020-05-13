# docker run -it --rm alpine /bin/ash
FROM alpine:latest

ARG api_branch=dev
ARG es_hostname=elasticsearch

# # centos:
# RUN yum install -y python3 python36-devel glibc-devel libev-devel gcc git
# alpine:
RUN apk update &&\
    apk upgrade &&\
    apk add --no-cache python3 py3-pip py3-setuptools musl-dev libxml2-dev libxslt-dev python3-dev libev-dev gcc git

# install and configure
WORKDIR /root
RUN git clone https://github.com/slub/efre-lod-api &&\
    cd efre-lod-api &&\
    git checkout ${api_branch} &&\
    git pull &&\
    python3 -m pip install -e .

COPY docker_apiconfig.yml /etc/lod-apiconfig.yml
run sed -i "s/localhost:9200/${es_hostname}:9200/g" /etc/lod-apiconfig.yml &&\
    sed -i "s/es_host:.*$/es_host: ${es_hostname}/g" /etc/lod-apiconfig.yml


EXPOSE 80
EXPOSE 8080

CMD lod-api start && tail -f /tmp/lod-api.log
