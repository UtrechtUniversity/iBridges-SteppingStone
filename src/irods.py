import subprocess
import json
import os
import sys
import irods.session
from irods.exception import CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME, CAT_NO_ACCESS_PERMISSION
from datetime import datetime
from typing import Union
from src.utils import print_error, print_warning, print_message

def read_irods_env(irods_env_file: str) -> dict:
    """
    Expects a json file in ~/.irods/irods_environment.json
    Returns the whole file as dictionary
    """
    with open(irods_env_file) as f:
        ienv = json.load(f)
    return ienv

def init_irods_connection(irods_env_file: str) -> Union[tuple, bool]:
    """
    Tests whether a connection to an irods server can be established.
    Expects an irods_environment.json file and a valid scrambled password .iRODS in ~/.irods.
    Creates a dictionary from the environment file.

    Returns: irods.session, dictionary
    """
    ienv = read_irods_env(irods_env_file=irods_env_file)
    res = subprocess.run(["ils"], input="bogus".encode(),
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.returncode == 0:
        print_message(f"Connected to: {ienv.get('irods_host')}")
        print_message(res.stdout.decode())
        session = irods.session.iRODSSession(irods_env_file=irods_env_file)
        return (session, ienv)
    else:
        print_error("ERROR: Cannot connect to iRODS server")
        print_message("Please do an iinit")
        return False

def irsync_irods_to_local(session: irods.session.iRODSSession, irodspath: str,
                          localpath: str) -> bool:
    """
    Given an iRODS path and a localpath, transfers data from iRODS to a local filesystem.
    During the transport checksums are checked on the fly and, if not present, registered in iRODS.
    Running time can be reduced by firsuring that checksums are already registered in iRODS
    (running "ichksum irodspath" on commandline).

    Returns: True upon success; False otherwise
    """
    if not os.path.isdir(localpath):
        print_error(f"ERROR: Destination {localpath} does not exist")
        return False
    else:
        itemname = os.path.basename(irodspath)
        if session.collections.exists(irodspath) or session.data_objects.exists(irodspath):
            res = subprocess.run(["irsync", "-Kr", f"i:{irodspath}", f"{localpath}/{itemname}"],
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res.returncode == 0:
                return True
            else:
                print_error(f"ERROR: Transferring {irodspath} --> {localpath} failed")
                print_message(res)
                return False
        else:
            print_error(f"ERROR: Transferring {irodspath} --> {localpath} failed")
            print_message("iRODS path not known.")
            return False

def get_irods_size(session: irods.session, path_names: list) -> int:
    """
    Calculates the cumulative file size of a list of iRODS paths. Paths can
    point toiRODS data objects or iRODS collections.

    Input: irods.session object, list of iRODS path names
    Output: cumulative sum of all file sizes
    """
    irods_sizes = []
    for path_name in path_names:
        if session.data_objects.exists(path_name):
            obj = session.data_objects.get(path_name)
            irods_sizes.append(obj.size)
        elif session.collections.exists(path_name):
            coll = session.collections.get(path_name)
            irods_sizes.append(sum((sum((obj.size for obj in objs)) for _, _, objs in coll.walk())))
    return sum(irods_sizes)

def map_collitems_to_local_path(session: irods.session, collpath: str, localpath: str) -> list:
    """
    irsync automatically creates subfolders etc, with this function we get
    the mapping from a collection to its absolute path in a folder.
    """
    coll = session.collections.get(collpath)
    destination = f"{localpath}/{os.path.basename(coll.path)}"
    objs = [obj for _, _, objs in coll.walk() for obj in objs]

    obj_to_file = []

    for obj in objs:
        obj_to_file.append((obj.path, destination+obj.path.split(coll.path)[1]))

    return obj_to_file

def annotate_data(session: irods.session, irodspath: str,
                  localpath: str, serverip: str):
    """
    Annnotates all data objects on the irodspath with metadata triple:
        "data_copy_on_server", serverip:localpath, timestamp

    Input: irods.session object, full irods path (coll or obj),
           full local path, server ip or fully qualified domain name
    Output: True when metadata is added or already present, False otherwise
    """
    if session.collections.exists(irodspath):
        coll = session.collections.get(irodspath)
        annotate_objs = [obj for _, _, objs in coll.walk() for obj in objs]
    elif session.data_objects.exists(irodspath):
        annotate_objs = [session.data_objects.get(irodspath)]
    else:
        print_error(f"ERROR: Annotating {irodspath} failed")
        print_message("Path does not exist.")
        return False

    timestamp = datetime.now()
    print(annotate_objs)
    for obj in annotate_objs:
        try:
            obj.metadata.add("data_copy_on_server", serverip+":"+localpath,
                             timestamp.strftime("%Y-%m-%d"))
        except CATALOG_ALREADY_HAS_ITEM_BY_THAT_NAME:
            print_warning("INFO: Metadata already exists {irodspath}")
        except CAT_NO_ACCESS_PERMISSION:
            print_error(f"ERROR: No permission to add metadata {irodspath}")
        except Exception:
            print_error(f"ERROR: Metadata could not be added {irodspath}")
