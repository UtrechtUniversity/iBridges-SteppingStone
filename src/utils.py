import configparser
import os
import csv
from typing import Union

def print_message(message: str, level: int=0) -> None:
    # INFO = 0
    # WARN = 1
    # ERROR = 2

    RED = '\x1b[1;31m'
    DEFAULT = '\x1b[0m'
    YEL = '\x1b[1;33m'
    BLUE = '\x1b[1;34m'

    if level==2:
        print(f"{RED}{message}{DEFAULT}")
    elif level==1:
        print(f"{YEL}{message}{DEFAULT}")
    else:
        print(f"{message}")

def print_error(message: str) -> None:
    print_message(message, level=2)

def print_warning(message: str) -> None:
    print_message(message, level=1)

def get_config(configfile: str) -> Union[tuple, bool]:
    if not os.path.isfile(configfile):
        print_error(f"ERROR reading config file {configfile}")
        print_message("\tFile does not exist.")
        return False

    config = configparser.ConfigParser()
    config.read_file(open(configfile))
    args = config._sections

    if 'remote' in args:
        datauser = config.get('remote', 'datauser')
        serverip = config.get('remote', 'serverip')
        sudo = config.getboolean('remote', 'sudo')
        cachelimit = (config.getfloat('local_cache', 'limit') * 1073741824) # GB to bytes
        return (datauser, serverip, sudo, cachelimit)
    else:
        print_error("ERROR config section expected: remote")
        return False

def read_source_dest_csv(filename: str) -> dict:
    source_to_dest = {}
    with open(filename, "r") as csv_file:
        csv_reader = csv.reader(csv_file)
        for line in csv_reader:
            try:
                source, dest = line[0].strip(), line[1].strip()
                if source and dest:
                    source_to_dest[source] = dest
                else:
                    print_warning(f"WARNING: Cannot read line in csv, skipping: {line}")
            except Exception:
                print_warning(f"WARNING: Cannot read line in csv, skipping: {line}")

    return source_to_dest

def create_dir(path: str) -> bool:
    """
    Creates a local directory, if it does not exist.
    Returns True upon succes or existence. False otherwise.
    """
    try:
        os.makedirs(path, exist_ok=True)
        return True
    except Exception:
        return False

def write_csv(success: list, 
              failure: list, 
              successpath: str, 
              failurepath: str) -> None:
    with open(successpath, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['iRODS', 'local'])
        for row in success:
            csv_out.writerow(row)

    print_message(f"Wrote succesful transfers to {successpath}")

    with open(failurepath, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(['iRODS', 'local', 'reason'])
        for row in failure:
            csv_out.writerow(row)

    print_message(f"Wrote failed transfers to {failurepath}")
