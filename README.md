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

Copy and configure `apiconfig.yml.example` to suit to your Elasticsearch-Infrastructure containing your JSON-LD processed by efre-elasticsearch-tools. Possible places for storing the config are:

* in `/etc` as `/etc/lod-apiconfig.yml`
* specify the config file directly via `-c`, e.g.
  ```
  lod-api -d --config apiconfig.yml
  ```

For starting the api in debug mode, do:
```
lod-api -d
```

For a productive environment, use:
```
lod-api [--config apiconfig.yml] {start|stop|restart}
```
and put it behind a load-balancer (like nginx).


# Tests

For triggering the tests, the api must be started separately. Tests depend on the configuration file of the api. Especially the debug-host and -port are important to determine where to run the tests against. The default configuration file is assumed to be `apiconfig.yml` in the home directory of the application. However, another configuration file can be provided using the `--config` switch. You can run the tests via
```
python3 -m pytest tests [--config apiconfig.yml]
```

If you want just a single test to be triggered, you can do this with
```
python3 -m pytest tests/test_to_do.py
```

If the output should be a bit more verbose, you can turn on print() statements via
```
python3 -m pytest -s tests/
```
