# Testing the LOD-API
Tests are composed at the moment of two test types:
1. [HTTP connection tests](#HTTP_connection_tests) (http\_status): Test are done to figure out wheather the api returns a http code 200 as response to requests against the different endpoints.
2. [Mock data tests](#Mock_data_tests) (mock\_data): Test that the data output does not change with respect to a predefined state of the mock data. The mock data are preloaded to an elasticsearch instance inside a docker container.

## HTTP connection tests
The connection tests are run against an already running instance of the lod-api. They simply check which the correct http status code in response to a request.
### requirements:
* running lod-api
### invocation
```sh
python3 -m pytest ./test_apis_http_status
```

## Mock data tests
### requirements:
* `docker` - to run elasticsearch with the mock data and optionally to run the lod-api in a different container

### preparation of docker images
In order to run elasticsearch inside a docker container, you probably have to increase the memory that can be consumed by the vm via `sudo sysctl -w vm.max_map_count=262144`.
#### build container
There are two containers that can be used:
1. elasticsearch container containing mock data
2. lod-api container running the api

All neccessary settings are set in [docker-compose.yml](./docker-compose.yml) and can be controlled there, e.g.
* elasticsearch version (default: `esmay_version: 7`)
* lod-api branch to be running inside the container (default: `api_branch: dev`)
* port mapping (default: lod-api running on port `8080`, elasticsearch on port `9200`)

```sh
docker-compose build [--no-cache]
```

### incovation
```sh
docker-compose up -d
python3 -m pytest --config ./docker/lod-api/docker_apiconfig.yml ./test_mock_data
```

If you want to run only the elasticsearch image and use your own instance of the LOD-API to query the data, just start elasticsearch
```sh
docker-compose up -d elasticsearch
```

### provide your own mock data
#### extract data
```sh
python3 data/genLDTestSet.py -s 10 -o ./data/ldj -c /etc/lod-apiconfig.yml
```
#### feed elasticsearch with them
```sh
bash data/reloadLDTestSet.sh data/ldj localhost
```
