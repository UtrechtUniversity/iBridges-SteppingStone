#!python3
import getopt
import sys
import os

import src.irods
import src.rsync
import src.utils

RED = '\x1b[1;31m'
DEFAULT = '\x1b[0m'
YEL = '\x1b[1;33m'
BLUE = '\x1b[1;34m'


def printHelp():
    print('Data transfer from iRODS thrigh stepping stone server to linux destination server')
    print('Usage: python3 transfer_workflow.py -i, --input=csv-file-path')
    print('Example: python3 transfer_workflow.py -i /home/user/transfer.csv')


def main(argv):
    try:
        opts, args = getopt.getopt(argv, "hi:", ["input="])
        if opts == []:
            print(RED+"ERROR: incorrect usage."+DEFAULT)
            printHelp()
            sys.exit(2)
    except getopt.GetoptError:
        print(RED+"ERROR: incorrect usage."+DEFAULT)
        printHelp()
        sys.exit(2)

    for opt, arg in opts:
        if opt == '-h':
            printHelp()
            sys.exit(2)
        elif opt in ['-i', '--input']:
            csvfile = arg
        else:
            printHelp()
            sys.exit(2)

    source_to_dest = src.utils.read_source_dest_csv(csvfile)
    if len(source_to_dest) == 0:
        print(RED+"Not a valid csv file, nothing to transfer", DEFAULT)
        print("\tExpected a comma-separated file")
        sys.exit(1)

    config = src.utils.get_config()
    datauser = config["remote"]["datauser"]
    serverip = config["remote"]["serverip"]
    sudo = config["remote"]["sudo"] == "True"
    cachelimit = float(config["local_cache"]["limit"]) * 1073741824  # GB to bytes

    # Check ssh connection and auth
    src.rsync.ssh_check_connection(datauser, serverip)

    # Create iRODS session
    session, ienv = src.irods.init_irods_connection()

    # Check if iRODS paths exist
    popkeys = []
    for key in source_to_dest:
        if not session.data_objects.exists(key) and not session.collections.exists(key):
            print(YEL+"WARNING: iRODS path does not exist:", key, DEFAULT)
            popkeys.append(key)
    [source_to_dest.pop(key) for key in popkeys]
    if len(source_to_dest) == 0:
        print(RED+"Nothing to transfer, check csv file", DEFAULT)
        sys.exit(1)

    # Execute transfer per key
    localcache = os.environ['HOME'] + "/irodscache"
    success = []  # tuple: source, destination
    failure = []  # triple: source, destination, fail reason

    if not src.utils.create_dir(localcache):
        print(RED+"ERROR: Cannot create local cache", localcache, DEFAULT)
        sys.exit(1)

    for key in source_to_dest:
        print("STATUS: Fetch data from iRODS", key, "-->", localcache)

        # Determine size of source
        size = src.irods.get_irods_size(session, [key])
        if size > cachelimit:
            print(YEL+"WARNING: Datasize exceeds cache size:", DEFAULT, key)
            failure.append((key, source_to_dest[key], "Exceeds cache"))
            continue

        # create destination folder on remote server
        mkdir_remote = src.rsync.create_remote_dir(datauser, serverip, sudo,
                                                   source_to_dest[key])
        if not mkdir_remote:
            print(RED+"ERROR: mkdir on remote server failed", source_to_dest[key], DEFAULT)
            failure.append((key, source_to_dest[key], "Creating remote dir failed"))
            continue

        # irsync data to stepping stone
        irods_success = src.irods.irsync_irods_to_local(session, key, localcache)
        if not irods_success:
            print(RED+"ERROR iRODS: transfer failed", key, localcache, DEFAULT)
            failure.append((key, source_to_dest[key], "iRODS transfer (irsync) failed"))
            continue

        # rsync data from stepping stone to destination server
        destination = source_to_dest[key]
        rsync_success = src.rsync.rsync_local_to_remote(
                datauser, serverip, sudo, localcache+"/"+os.path.basename(key), destination)
        if rsync_success:
            success.append((key, source_to_dest[key]))
            if session.collections.exists(key):
                success.extend(src.irods.map_collitems_to_local_path(session, key, destination))

            # Create iRODS metadata entry
            # print("DEBUG: annotate", key)
            # src.irods.annotate_data(session, key, destination+"/"+os.path.basename(key), serverip)
            # Delete cache
            src.rsync.empty_dir(localcache)
        else:
            print(RED+"ERROR rsync: transfer failed",
                  localcache+"/"+os.path.basename(key),
                  os.path.dirname(destination), DEFAULT)
            failure.append((key, source_to_dest[key], "rsync to remote failed"))
            continue
    src.utils.write_csv(success, failure)


if __name__ == "__main__":
    main(sys.argv[1:])
