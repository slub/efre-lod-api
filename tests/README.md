# Testing the LOD-API
## requirements
* docker
* esbulk
* curl

## Using Docker and mock data
```sh
cd ./tests
# extract mock data
tar -xjf data/LDTestSet.tar.bz2 -C data
# start docker service for lod-api and elasticsearch
docker-compose up -d 
# load mock data into elasticsearch (esbulk required)
bash data/reloadLDTestSet.sh data/ldj localhost
```
