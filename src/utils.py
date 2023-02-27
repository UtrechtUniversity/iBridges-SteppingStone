import configparser
import os
import sys
from datetime import datetime
import csv

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
                    source_to_dest[source] = dest
                else:
                    print(YEL+"WARNING: Cannot read line in csv, skipping: ", DEFAULT, line)
            except Exception:
                print(YEL+"WARNING: Cannot read line in csv, skipping: ", DEFAULT, line)
    return source_to_dest


def create_dir(path: str) -> bool:
    """
    Creates a local directory, if it does not exist.
    Returns True upon succes or existence. False otherwise.
    """
    if not os.path.exists(path):
        try:
            os.makedirs(path)
            return True
        except Exception:
            return False
    else:
        return True


def write_csv(success: list, failure: list):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
    successpath = os.environ['HOME'] + "/output_irods_data_transfer_"+timestamp+'.csv'
    failurepath = os.environ['HOME'] + "/error_irods_data_transfer_"+timestamp+'.csv'

    with open(successpath, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['iRODS', 'local'])
        for row in success:
            csv_out.writerow(row)

    with open(failurepath, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['iRODS', 'local', 'reason'])
        for row in failure:
            csv_out.writerow(row)
