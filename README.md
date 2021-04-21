<img alt="EFRE-Lod logo" src="https://raw.githubusercontent.com/slub/data.slub-dresden.de/master/assets/images/EFRE_EU_quer_2015_rgb_engl.svg" width="300" >

# efre-lod-api
Flask API to access your Linked-Open-Data contained in an elasticsearch-cluster.

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

## Systemd unit-file (optional)
The systemd template file can be used to start the lod-api via systemd even automatically at boot time.
* copy systemd template file to `/etc/systemd/system`.
  ```
  cp systemd/lod-api@.service /etc/systemd/system/lod-api@username.service
  ```
  hereby, replace the `username` with the user which should finally run the LOD-API and has it locally installed to `/home/username/.local/bin`

## via Makefile (optional)
There is the posibility to use `make` to install the software into your home-directory (not using a virtualenv) and setup the systemd unit file. You must run the `make` as root in the project's base directory.

# Usage

Copy and configure `apiconfig.yml.example` to suit to your Elasticsearch-Infrastructure containing your JSON-LD. Possible places for storing the config are:

* in `/etc` as `/etc/lod-apiconfig.yml`
* specify the config file directly via `-c`, e.g.
  ```
  lod-api -d --config apiconfig.yml
  ```

For starting the api in debug mode, do:
```
lod-api -d
```

For controlling the daemon, use:
```
lod-api [--config apiconfig.yml] {start|stop|restart}
```

For a productive environment, we recommend to put the API behind a load-balancer (like nginx).

## systemd

Enable the lod-api-systemd-service to start it at boot using the `username` of the local user which installed the `lod-api` software.
```
systemctl enable lod-api@username
```

And start/stop it via systemctl:
```
service lod-api@username {start|stop|restart|status}
```




## Example

One example configuration file can be found in `tests/docker/lod-api/docker_apiconfig.yml`. This configuration fits to the data contained in the test set `tests/data/LDTestSet.tar.bz2`, which can be stored in an elasticsearch and be used as test set for the lod-api. See [data mocking integration](#data_mocking_integration).


# Tests

For triggering the tests, the api must be started separately. Tests depend on the configuration file of the api. Especially the debug-host and -port are important to determine where to run the tests against. The default configuration file is assumed to be `apiconfig.yml` in the home directory of the application. However, another configuration file can be provided using the `--config` switch. You can run the tests via
```
python3 -m pytest tests [--config apiconfig.yml]
```

**Be aware** though that some of the tests depend on mock data which are provided in `tests/data` and have to be stored in a local elasticsearch instance. When you want to test the API with your own data you can, of course, generate your own test set of mock data

If you want just a single test to be triggered, e.g. the connection test, you can do this with
```
python3 -m pytest tests/test_apis_http_status
```

If the output should be a bit more verbose, you can turn on print() statements via
```
python3 -m pytest -s tests/
```
For more information on the tests have a look into [tests](tests/README.md).

## Data mocking integration

We provide an elasticsearch Dockerfile (in `tests/docker/elasticsearch`) from which a container running an elasticsearch instance can be build. There are also test set with linked data (in `tests/data/LDTestSet.tar.bz2` which can be extraced and loaded into the locally running elasticsearch. See [tests](tests/README.md) for more information on howto prepare the elasticsearch docker image.

In the case you are using the mock data test set you will most likely use its provided configuration file as well for the api:
```sh
lod-api -d -c tests/docker/lod-api/docker-apiconfig.yml
```
