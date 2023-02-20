import subprocess
import sys

RED = '\x1b[1;31m'
DEFAULT = '\x1b[0m'
YEL = '\x1b[1;33m'
BLUE = '\x1b[1;34m'

serverip = "145.38.187.136"
datauser = "cstaiger"
destpath = "/home/cstaiger/mydata"
sourcepath = "/Users/staig001/testdata"
sudo = False


def ssh_check_connection(datauser: str, serverip: str):
    """
    Check ssh datauser@serverip and execute uname -a.
    """
    ssh = subprocess.run(["ssh", datauser+"@"+serverip, "uname -a"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ssh.stderr:
        print(RED)
        print("Connection failed: ", datauser, serverip, DEFAULT)
        print(ssh.stderr.decode())
        sys.exit(1)
    else:
        print(BLUE, "Connected: ", datauser, serverip, DEFAULT)


def rsync_local_to_remote(datauser: str, serverip: str, sudo: bool,
                          sourcepath: str, destpath: str) -> str:
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

    Returns: '' (success), sourcepath (failure)
    """

    print("Uploading data: %s --> %s@%s:%s" % (sourcepath, datauser, serverip, destpath))
    if sudo:
        res = subprocess.run(['rsync', '--rsync-path="sudo rsync"', '-rc', sourcepath,
                             datauser+"@"+serverip+':'+destpath],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        res = subprocess.run(['rsync', '-rc',
                             sourcepath, datauser+"@"+serverip+':'+destpath],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if res.stderr:
        print(RED, "rsync failed:", DEFAULT)
        print(res.stderr)
        return sourcepath
    elif res.stdout:
        print(YEL, "\t --> Data transfer complete", DEFAULT)
        return ''
