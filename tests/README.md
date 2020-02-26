# Testing the LOD-API
## requirements for data mocking
* docker
* esbulk
* curl

## Using Docker and mock data
In order to run elasticsearch inside a docker container, you probably have to increase the memory that can be consumed by the vm via `sudo sysctl -w vm.max_map_count=262144`.

```sh
cd ./tests
# extract mock data
tar -xjf data/LDTestSet.tar.bz2 -C data
# start docker service for lod-api and elasticsearch
docker-compose up -d elasticsearch
# load mock data into elasticsearch (esbulk required)
bash data/reloadLDTestSet.sh data/ldj localhost
```

or all together:
`tar -xjf data/LDTestSet.tar.bz2 -C data && docker-compose up -d elasticsearch && bash data/reloadLDTestSet.sh data/ldj localhost`

## Running tests
