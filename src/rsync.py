import subprocess
from pathlib import Path
from shutil import rmtree
from src.utils import print_error, print_warning, print_message, print_success

def ssh_check_connection(datauser: str, serverip: str) -> bool:
    """
    Check ssh datauser@serverip and execute uname -a.
    """
    ssh = subprocess.run(["ssh", "-o ConnectTimeout=30", f"{datauser}@{serverip}", "uname -a"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ssh.stderr:
        print_error(f"Connection failed: {datauser}@{serverip}")
        print_message(ssh.stderr.decode())
        return False

    print_success(f"Connected: {datauser}@{serverip}")
    return True

def create_remote_dir(datauser: str, serverip: str, sudo: bool, dirpath: str) -> bool:
    """
    Creates a folder on a remote server.
    dirpath: full absolute path
    Returns: True upon success
    """
    print(f"Ensure directory: {serverip}:{dirpath}")
    mkdir = subprocess.run(["ssh", f"{datauser}@{serverip}", "mkdir -p", dirpath],
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if mkdir.stderr:
        print_error(f"mkdir failed: {datauser} {serverip} {dirpath}")
        print_message(mkdir.stderr.decode())
        return False

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

    print_message(f"Uploading data: {sourcepath} --> {datauser}@{serverip}:{destpath}")
    if sudo:
        res = subprocess.run(['rsync', '--rsync-path="sudo rsync"',
                              '-rc --relative', sourcepath,
                              f"{datauser}@{serverip}:{destpath}"],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    else:
        res = subprocess.run(['rsync', '-rc', sourcepath,
                              f"{datauser}@{serverip}:{destpath}"],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if res.stderr:
        print_error(f"rsync failed: {str(res.stderr)}")
        return False

    print_warning("--> Data transfer complete")
    return True
