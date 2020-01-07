# efre-lod-api
Flask API to access the elasticsearch-data transformed by efre-lod-elasticsearch tools

# Requirements

python3 and other packages specified in requirements.txt

# Install

* clone repository
  ```
  git clone https://github.com/efre-lod/efre-lod-api.git
  ```

* create and activate a virtual environment for this project
  ```
  cd efre-lod-api
  python3 -m venv env
  source ./env/bin/activate
  ```

* install package and its requirements into venv
  ```
  pip3 install -e .
  ```

# Usage

Copy and configure `apiconfig.json.example` to suit to your Elasticsearch-Infrastructure containing your JSON-LD processed by efre-elasticsearch-tools. Possible places for storing the config are:

* in `/etc` as `/etc/lod-apiconfig.json`
* specify the config file directly via `-c`, e.g.
  ```
  lod-api -d --config apiconfig.json
  ```

For starting the api in debug mode, do:
```
lod-api -d
```

For a productive environment, use:
```
lod-api {start|stop|restart}
```
and put it behind a load-balancer (like nginx).


# Tests

For triggering the tests, do
```
python3 -m pytest tests/
```
