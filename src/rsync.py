import subprocess
from pathlib import Path
from shutil import rmtree
from src.utils import print_error, print_warning, print_message, print_success


def ssh_check_connection(datauser: str, serverip: str) -> bool:
    """
    Check ssh datauser@serverip and execute uname -a.
    """
    ssh = subprocess.run(["ssh", "-o ConnectTimeout=30", f"{datauser}@{serverip}", "uname -a"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
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
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
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


def remote_path_exists(user: str, server: str, path: str) -> bool:
    res = subprocess.run(["ssh", f"{user}@{server}", f"ls {path}"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if res.stderr:
        return False
    else:
        return True


def is_remote_dir(user: str, server: str, path: str) -> bool:
    res = subprocess.run(["ssh", f"{user}@{server}", f"test -d { path } && echo 'Directory Exists'"],
                         stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    if res.stdout == "Directory Exists":
        return True
    elif res.stderr:
        return False
    else:
        return False


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
        res = subprocess.run(['rsync', '--rsync-path="sudo rsync"', '-rc --relative', sourcepath,
                             f"{datauser}@{serverip}:{destpath}"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    else:
        res = subprocess.run(['rsync', '-rc', sourcepath,
                             f"{datauser}@{serverip}:{destpath}"],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)

    if res.stderr:
        print_error(f"rsync failed: {str(res.stderr)}")
        return False

    return True


def rsync_remote_to_local(datauser: str, serverip: str, sudo: bool,
                          sourcepath: str, destpath: str) -> bool:
    """
    Transfers data from a remote server to a local server through rsync.
    Assumes that an ssh keypair was installed for that user beforehand (local priv/pub key
    and remote authorized_keys files are setup).
    Returns: True (success), False (failure)
    """

    print_message(f"Downloading data: {datauser}@{serverip}:{sourcepath} --> {destpath}")
    if sudo:
        res = subprocess.run(['rsync', '--rsync-path="sudo rsync"', '-rc --relative',
                             f"{datauser}@{serverip}:{sourcepath}", destpath],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
    else:
        res = subprocess.run(['rsync', '-rc',
                             f"{datauser}@{serverip}:{sourcepath}", destpath],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)

    if res.stderr:
        print_error(f"rsync failed: {str(res.stderr)}")
        return False

    return True


def get_remote_size(user: str, server: str, path_names: list) -> int:
    """
    Checks cumulative file size of all files in the list path_names on the remote servere.
    The list can also contain directories.
    Params:
        user: remote user name
        server: FQDN or IP address
        path_names: list of absolute paths on the remote server
    Returns: cumulative file size
    """
    size = 0
    for path in path_names:
        res = subprocess.run(['ssh', f'{user}@{server}', 'du', '-bs', f'{path}'],
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if res.stdout:
            path_size = int(res.stdout.split()[0].decode())
            size = size + path_size
        else:
            print_error(f"Cannot determine size: {path}")
            print_error(f"{res.stderr}")

    return size
