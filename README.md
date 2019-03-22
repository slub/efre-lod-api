# efre-lod-api
Flask API to acess the elasticsearch-data transformed by efre-lod-elasticsearch tools
# Requirements

python3 and other packages specified in requirements.txt

# Install

```
git clone https://github.com/efre-lod/efre-lod-api.git
cd efre-lod-api
pip3 install -r requirements.txt
```

# Usage

Edit apiconfig.json to suit to your Elasticsearch-Infrastructure containing your JSON-LD processed by efre-elasticsearch-tools. 

For debug purposes, do:
```
python3 flask_api.py
```

For a productive environment, use:
```
./run_via_bjoern.py {start|stop|restart}
```
and put it behind a load-balancer (like nginx).



