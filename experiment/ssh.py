RED = '\x1b[1;31m'
DEFAULT = '\x1b[0m'
YEL = '\x1b[1;33m'
BLUE = '\x1b[1;34m'

serverip = "145.38.187.136"
datauser = "cstaiger"
destpath = "/home/cstaiger/mydata"
sourcepath = "/Users/staig001/testdata"

import subprocess
import sys
#rsync --rsync-path="sudo rsync" <LOCALFILE> USER@SERVER2:/root

def ssh_check_connection(datauser, serverip):
    ssh = subprocess.run(["ssh", datauser+"@"+serverip, "uname -a"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if ssh.stderr:
        print(RED)
        print("Connection failed: ", datauser, serverip, DEFAULT)
        print(ssh.stderr.decode())
        sys.exit(1)
    else:
        print(BLUE, "Connected: ", datauser, serverip, DEFAULT)

def rsync_local_to_remote(datauser, serverip, sourcepath, destpath):
    print("Uploading data: %s --> %s@%s:%s" %(sourcepath, datauser, serverip, destpath))
    res = subprocess.run(["rsync", "-rc", sourcepath, datauser+"@"+serverip+":"+destpath], 
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if res.stderr:
        print(RED, "rsync failed:", DEFAULT)
        print(res.stderr)
        return(sourcepath)
    elif res.stdout:
        print(YEL, "\t --> Data transfer complete", DEFAULT)
        return
