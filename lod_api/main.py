import argparse
import os
import sys
import bjoern

import lod_api
from lod_api.helper_functions import ConfigParser
from lod_api.daemonize import handler


parser = argparse.ArgumentParser()

parser.add_argument("-c", "--config", nargs=1,
                    help="configuration file")

parser.add_argument("-d", "--debug", action='store_true',
                    help="use flask\'s debug mode")

parser.add_argument("action", nargs="?",
                    choices=["start", "stop", "restart"],
                    help="action for daemon start|stop|restart")

args = parser.parse_args()


def read_config(config_file=None, 
                conffile_default="/etc/lod-apiconfig.json"):
    """ Read the config file for the LOD-API and store it in the
        module lod_api.CONFIG for global usage"""
    if not config_file:
        if args.config:
            # use via argument-provided config file
            config_file = args.config[0]
        elif os.path.isfile(conffile_default):
            # use a global config file in /etc
            config_file = conffile_default
        else:
            print("please provide a valid config file. "
                  "Either via \"{}\" or using the -c/--config switch".format(
                      conffile_default)
                  )
            print(" ")
            parser.print_help()
            sys.exit(1)

    lod_api.CONFIG = ConfigParser(config_file)


def main():
    # it is important to read the config first via
    # `lod_api.CONFIG = ConfigParser()` and just after that
    # import the flask_api from lod_api. Otherwise config settings
    # would be unknown to the api.
    read_config()
    from lod_api import flask_api
    if args.debug:
        flask_api.main()
    else:
        # start 
        if not args.action:
            print("Either start app in debugging mode (-d) or provide an"
                  "valid action for the daemon (start|stop|restart)")
            print(" ")
            parser.print_help()
            sys.exit(1)
        else:
            handler(args.action,
                    stdout='/tmp/daemonize.log',
                    pidfile='/tmp/daemonize.pid')
            host = lod_api.CONFIG.get("apihost")
            bjoern.run(lod_api.app, host, 80)


if __name__ == '__main__':
    main()
