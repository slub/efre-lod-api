import argparse
import os
import sys
import bjoern

import lod_api
from lod_api.helper_functions import ConfigParser
from lod_api.daemonize import handler


def parse_arguments():
    """ parse arguments via argparse """
    parser = argparse.ArgumentParser()

    parser.add_argument("-c", "--config", nargs=1,
                        help="configuration file")

    parser.add_argument("-d", "--debug", action='store_true',
                        help="use flask\'s debug mode")

    parser.add_argument("action", nargs="?",
                        choices=["start", "stop", "restart"],
                        help="action for daemon start|stop|restart")
    return parser


def read_config(config_file=None,
                conffile_default="/etc/lod-apiconfig.json"):
    """ Read the config file for the LOD-API and store it in the
        module lod_api.CONFIG for global usage"""

    if config_file and os.path.isfile(config_file):
        # use via argument-provided config file
        config_file = config_file
    elif os.path.isfile(conffile_default):
        # use a global config file in /etc
        config_file = conffile_default
    else:
        print("Please provide a valid config file. \n"
              "Either via \"{}\" or using the -c/--config switch".format(
                  conffile_default)
              )
        sys.exit(1)

    lod_api.CONFIG = ConfigParser(config_file)


def start_api(debug=True, action=None, config_file=None):
    # it is important to read the config first via
    # `lod_api.CONFIG = ConfigParser()` and just after that
    # import the flask_api from lod_api. Otherwise config settings
    # would be unknown to the api.
    read_config(config_file)
    from lod_api import flask_api
    if debug:
        flask_api.run_app()
    else:
        if not action:
            print("Either start app in debugging mode (-d) or provide an"
                  "valid action for the daemon (start|stop|restart)")
            sys.exit(1)
        else:
            handler(action,
                    stdout='/tmp/daemonize.log',
                    pidfile='/tmp/daemonize.pid')
            host = lod_api.CONFIG.get("apihost")
            bjoern.run(lod_api.app, host, 80)


def main():
    parser = parse_arguments()
    args = parser.parse_args()

    if args.config:
        config = args.config[0]
    else:
        config = None

    try:
        start_api(debug=args.debug, action=args.action,
                  config_file=config)
    except SystemExit as ex:
        if ex.code == 1:
            print(" ")
            parser.print_help()
            sys.exit(1)


if __name__ == '__main__':
    main()
