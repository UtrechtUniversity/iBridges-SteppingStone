#!python3
import argparse
# import getopt
import os
from datetime import datetime

import src.irods
import src.rsync
import src.utils

from src.utils import print_error, print_warning, print_message


class iBridgesSteppingStone:

    def __init__(self,
                 transfer_config,
                 input_csv,
                 output_folder) -> None:
        self.transfer_config = transfer_config
        self.input_csv = input_csv
        self.output_folder = output_folder

    @classmethod
    def from_arguments(cls):
        parser = argparse.ArgumentParser(
            prog='python transfer_workflow.py',
            description='Transfers data from Yoda/iRODS to a destination server through a stepping stone server.',
            epilog='Usage example: python transfer_workflow.py -i /home/user/transfer.csv'
            )

        default_xfr_cfg = os.path.join(str(os.getenv('HOME')), '.irods', 'transfer.config')

        parser.add_argument('--input', '-i', type=str,
                            help='path to .CSV-file containing one "source, target"-pair per line',
                            required=True)
        parser.add_argument('--output', '-o', type=str, 
                            help='folder to write data transfer logs to',
                            default="./")
        parser.add_argument('--config', '-c', type=str,
                            help=f'path to iRods transfer config (default: {default_xfr_cfg})', 
                            default=default_xfr_cfg)

        args = parser.parse_args()

        return cls(
            input_csv=args.input,
            output_folder=args.output,
            transfer_config=args.config)
    
    # def printHelp():
    #     print('Data transfer from iRODS thrigh stepping stone server to linux destination server')
    #     print('Usage: python3 transfer_workflow.py -i, --input=csv-file-path')
    #     print('Example: python3 transfer_workflow.py -i /home/user/transfer.csv')


    def transfer(self):
        # try:
        #     opts, args = getopt.getopt(argv, "hi:", ["input="])
        #     if opts == []:
        #         print(RED+"ERROR: incorrect usage."+DEFAULT)
        #         printHelp()
        #         sys.exit(2)
        # except getopt.GetoptError:
        #     print(RED+"ERROR: incorrect usage."+DEFAULT)
        #     printHelp()
        #     sys.exit(2)

        # for opt, arg in opts:
        #     if opt == '-h':
        #         printHelp()
        #         sys.exit(2)
        #     elif opt in ['-i', '--input']:
        #         csvfile = arg
        #     else:
        #         printHelp()
        #         sys.exit(2)

        source_to_dest = src.utils.read_source_dest_csv(filename=self.input_csv)

        if len(source_to_dest) == 0:
            print_error("Nothing to transfer")
            print_message("\tEmpty file, or not a CSV-file")
            exit(1)

        config = src.utils.get_config(configfile=self.transfer_config)

        if config:
            datauser, serverip, sudo, cachelimit = config
        else:
            exit(1)

        # Check ssh connection and auth
        src.rsync.ssh_check_connection(datauser, serverip)

        # Create iRODS session
        session, ienv = src.irods.init_irods_connection()

        # Check if iRODS paths exist
        popkeys = []
        for key in source_to_dest:
            if not session.data_objects.exists(key) and not session.collections.exists(key):
                print_warning(f"WARNING: iRODS path does not exist: {key}")
                popkeys.append(key)
        [source_to_dest.pop(key) for key in popkeys]
        if len(source_to_dest) == 0:
            print_error("Nothing to transfer, check csv file")
            exit(1)

        # Execute transfer per key
        localcache = os.environ['HOME'] + "/irodscache"
        success = []  # tuple: source, destination
        failure = []  # triple: source, destination, fail reason

        if not src.utils.create_dir(localcache):
            print_error(f"ERROR: Cannot create local cache {localcache}")
            exit(1)

        for key in source_to_dest:
            print_message(f"STATUS: Fetch data from iRODS {key} --> {localcache}")

            # Determine size of source
            size = src.irods.get_irods_size(session, [key])
            if size > cachelimit:
                print_warning(f"WARNING: Datasize exceeds cache size: {key}")
                failure.append((key, source_to_dest[key], "Exceeds cache"))
                continue

            # create destination folder on remote server
            mkdir_remote = src.rsync.create_remote_dir(datauser, serverip, sudo,
                                                    source_to_dest[key])
            if not mkdir_remote:
                print_error(f"ERROR: mkdir on remote server failed {source_to_dest[key]}")
                failure.append((key, source_to_dest[key], "Creating remote dir failed"))
                continue

            # irsync data to stepping stone
            irods_success = src.irods.irsync_irods_to_local(session, key, localcache)
            if not irods_success:
                print_error(f"ERROR iRODS: transfer failed {key} {localcache}")
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
                print_error(f"ERROR rsync: transfer failed {localcache+'/'+os.path.basename(key)} {os.path.dirname(destination)}")
                failure.append((key, source_to_dest[key], "rsync to remote failed"))
                continue

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")        
        successpath = os.path.join(self.output_folder, f'output_irods_data_transfer_{timestamp}.csv')
        failurepath = os.path.join(self.output_folder, f'error_irods_data_transfer_{timestamp}.csv')

        src.utils.write_csv(success=success, 
                            failure=failure, 
                            successpath=successpath, 
                            failurepath=failurepath)


if __name__ == "__main__":

    bridge = iBridgesSteppingStone.from_arguments()
    # or
    # bridge = iBridgesSteppingStone(input_csv='/data/ibridges/test.csv', transfer_config='...', output_folder='...')

    bridge.transfer()

