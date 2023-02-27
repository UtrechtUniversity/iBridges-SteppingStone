import subprocess
import sys
from pathlib import Path
from shutil import rmtree

RED = '\x1b[1;31m'
DEFAULT = '\x1b[0m'
YEL = '\x1b[1;33m'
BLUE = '\x1b[1;34m'


def ssh_check_connection(datauser: str, serverip: str):
    """
    Check ssh datauser@serverip and execute uname -a.
    """
    ssh = subprocess.run(["ssh", "-o ConnectTimeout=30", datauser+"@"+serverip, "uname -a"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ssh.stderr:
        print(RED+"Connection failed: ", datauser, serverip, DEFAULT)
        print(ssh.stderr.decode())
        sys.exit(1)
    else:
        print(BLUE, "Connected: ", datauser, serverip, DEFAULT)


def create_remote_dir(datauser: str, serverip: str, sudo: bool, dirpath: str) -> bool:
    """
    Creates a folder on a remote server.
    dirpath: full absolute path
    Returns: True upon success
    """
    print("Ensure directory: %s:%s" % (serverip, dirpath))
    mkdir = subprocess.run(["ssh", datauser+"@"+serverip, "mkdir -p", dirpath],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if mkdir.stderr:
        print(RED+"mkdir failed: ", datauser, serverip, dirpath, DEFAULT)
        print(mkdir.stderr.decode())
        return False
    else:
        return True


def empty_dir(directory: str):
    for path in Path(directory).glob("**/*"):
        if path.is_file():
            path.unlink()
        elif path.is_dir():
            rmtree(path)


def rsync_local_to_remote(datauser: str, serverip: str, sudo: bool,
                          sourcepath: str, destpath: str) -> bool:
    """
    Transfers data from a local server to a remote linux server through rsync.
    Assumes that an ssh keypair was installed for that user beforehand (local priv/pub key
    and remote authorized_keys files are setup).

    datauser: username on remote server
    serverip: ip or fully qualified domain name of the remote server
    sudo: if the destination on the remote server is protected to be written by the datauser,
          sudo is needed to overrule that (not recommended)
    sourcepath: local data path, can be file or folder
    destpath: destination folder on remote server

    Returns: True (success), False (failure)
    """

    print("Uploading data: %s --> %s@%s:%s" % (sourcepath, datauser, serverip, destpath))
    if sudo:
        res = subprocess.run(['rsync', '--rsync-path="sudo rsync"',
                             '-rc --relative', sourcepath,
                              datauser+"@"+serverip+':'+destpath],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        res = subprocess.run(['rsync', '-rc',
                             sourcepath, datauser+"@"+serverip+':'+destpath],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if res.stderr:
        print(RED+"rsync failed:", DEFAULT)
        print(res.stderr)
        return False
    else:
        print(YEL+"\t --> Data transfer complete", DEFAULT)
        return True
