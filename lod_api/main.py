import argparse

import lod_api
from lod_api.helper_functions import ConfigParser


def read_config(config_file=None):
    if not config_file:
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--config", nargs=1,
                            required=True, help="configuration file")
        args = parser.parse_args()
        config_file = args.config[0]

    lod_api.CONFIG = ConfigParser(config_file)


def run_app():
    from lod_api import flask_api
    flask_api.main()


def main():
    read_config()
    run_app()


if __name__ == '__main__':
    main()
