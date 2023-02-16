import subprocess
import json
import os
import sys
import irods

RED = '\x1b[1;31m'
DEFAULT = '\x1b[0m'
YEL = '\x1b[1;33m'
BLUE = '\x1b[1;34m'

def read_irods_env() -> dict:
    envFile = os.environ['HOME'] + "/.irods/irods_environment.json"
    if os.path.exists(envFile):
        with open(envFile) as f:
            ienv = json.load(f)
        return ienv
    else:
        print(RED+"ERROR:", os.environ['HOME']+"/.irods/irods_environment.json not found", DEFAULT)
        sys.exit(1)

def test_irods_connection() -> tuple:
    ienv = read_irods_env()
    res = subprocess.run(["ils"], input="bogus".encode(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if res.returncode == 0:
        print("Connected to:", ienv.get('irods_host'))
        print(res.stdout.decode())
        session = irods.session.iRODSSession(irods_env_file = os.environ['HOME']+"/.irods/irods_environment.json")
        return (session, ienv)
    else:
        print(RED+"ERROR: Cannot connect to iRODS server", DEFAULT)
        print("Please do an iinit")
        sys.exit(1)

def irsync_irods_to_local(irodspath: str, localpath=os.environ['HOME']+"/dump") -> bool:
    print("DEBUG: Transferring %s --> %s" %(irodspath, localpath))
    
    if not os.path.isdir(localpath):
        print(RED+"ERROR: Destination", localpath, "does not exist", default)
        sys.exit(1)
    else:
        itemname = os.path.basename(irodspath)
        if session.collections.exists(irodspath) or session.data_objects.exists(irodspath):
            res = subprocess.run(["irsync", "-Kr", "i:"+irodspath, localpath+"/"+itemname], 
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if res.returncode == 0:
                return True
            else:
                print(RED+"ERROR: Transferring %s --> %s failed" %(irodspath, localpath), DEFAULT)
                print(res)
                return False
        else:
            print(RED+"ERROR: Transferring %s --> %s failed" %(irodspath, localpath), DEFAULT)
            print("\t iRODS path not known.")
            return False


def get_irods_size(session: irods.session.iRODSSession, path_names: list) -> int:
    irods_sizes = []
    for path_name in path_names:
        if session.data_objects.exists(path_name):
            obj = session.data_objects.get(path_name)
            irods_sizes.append(obj.size)
        elif session.collections.exists(path_name):
            coll = session.collections.get(path_name)
            irods_sizes.append(sum((sum((obj.size for obj in objs)) for _, _, objs in coll.walk())))
    return sum(irods_sizes)
