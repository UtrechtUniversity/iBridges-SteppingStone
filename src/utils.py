import configparser
import os
import sys

RED = '\x1b[1;31m'
DEFAULT = '\x1b[0m'
YEL = '\x1b[1;33m'
BLUE = '\x1b[1;34m'


def get_config(configfile=os.environ['HOME'] + "/.irods/transfer.config") -> dict:
    if not os.path.isfile(configfile):
        print(RED+"ERROR reading config file", configfile, DEFAULT)
        print("\t File does not exist.")
        sys.exit(1)

    config = configparser.ConfigParser()
    config.read_file(open(configfile))
    args = config._sections
    if 'remote' in args:
        return args
    else:
        print(RED+"ERROR config section expected: remote", DEFAULT)
        sys.exit(1)


def read_source_dest_csv(csvfile: str) -> dict:
    source_to_dest = {}
    with open(csvfile, "r") as csv:
        for line in csv:
            try:
                source = line.split(',')[0].strip()
                dest = line.split(',')[1].strip()
                if source != '' and dest != '':
                    source_to_dest[line.split(',')[0]] = line.split(',')[1]
                else:
                    print(RED+"ERROR: Cannot read line in csv, skipping: ", DEFAULT,  line)
            except Exception:
                print(RED+"ERROR: Cannot read line in csv, skipping: ", DEFAULT,  line)
    return source_to_dest
