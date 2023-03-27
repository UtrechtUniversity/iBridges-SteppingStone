#!python3
import argparse
import os
import sys
from datetime import datetime

import src.irods_functions
import src.rsync
import src.utils

from src.utils import print_error, print_warning, print_message


class iBridgesSteppingStone:

    def __init__(self,
                 transfer_config: str,
                 irods_env_file: str,
                 input_csv: str,
                 output_folder: str, 
                 operation: str) -> None:

        for file in [irods_env_file, transfer_config, input_csv]:
            if not os.path.exists(file):
                print_error(f"ERROR: {file} does not exist")
                sys.exit(1)

        self.irods_env_file = irods_env_file
        self.transfer_config = transfer_config
        self.input_csv = input_csv
        self.output_folder = output_folder
        self.operation = operation

    @classmethod
    def from_arguments(cls):
        parser = argparse.ArgumentParser(
            prog='python transfer_workflow.py',
            description='Transfers data between Yoda/iRODS and a destination server through '
                        +'a stepping stone server',
            epilog='Usage example: python transfer_workflow.py -i /home/user/transfer.csv -p export'
            )

        default_xfr_cfg = os.path.join(str(os.getenv('HOME')), '.irods', 'transfer.config')
        default_irods_env = os.path.join(str(os.getenv('HOME')), '.irods', 'irods_environment.json')

        parser.add_argument('--input', '-i', type=str,
                            help='path to .CSV-file containing one "source, target"-pair per line',
                            required=True)
        parser.add_argument('--output', '-o', type=str,
                            help='folder to write data transfer logs to',
                            default="./")
        parser.add_argument('--config', '-c', type=str,
                            help=f'path to iRods transfer config (default: {default_xfr_cfg})',
                            default=default_xfr_cfg)
        parser.add_argument('--env', '-e', type=str,
                            help=f'path to iRods environment config (default: {default_irods_env})',
                            default=default_irods_env)
        parser.add_argument('--operation', '-p', type=str,
                            help=f'export (iRODS/YODA to remote server, import (remote server to iRODS/YODA)')


        args = parser.parse_args()

        return cls(
            input_csv=args.input,
            output_folder=args.output,
            transfer_config=args.config,
            irods_env_file=args.env,
            operation=args.operation)

    def export(self):
        source_to_dest = src.utils.read_source_dest_csv(filename=self.input_csv)

        if len(source_to_dest) == 0:
            print_error("Nothing to transfer")
            print_message("Empty file, or not a CSV-file")
            sys.exit(1)

        config = src.utils.get_config(configfile=self.transfer_config)

        if config:
            datauser, serverip, sudo, cachelimit = config
        else:
            sys.exit(1)

        # Check ssh connection and auth
        if not src.rsync.ssh_check_connection(datauser, serverip):
            sys.exit(1)

        # Create iRODS session
        irods_conn = src.irods_functions.init_irods_connection(irods_env_file=self.irods_env_file)

        if irods_conn:
            session, _ = irods_conn
        else:
            sys.exit(1)

        # Check if iRODS paths exist
        for key in list(source_to_dest.keys()):
            if not session.data_objects.exists(key) and not session.collections.exists(key):
                print_warning(f"WARNING: iRODS path does not exist: {key}")
                del source_to_dest[key]

        if len(source_to_dest) == 0:
            print_error("Nothing to transfer, check CSV-file")
            sys.exit(1)

        # Execute transfer per key
        localcache = os.getenv('HOME') + "/irodscache"
        success = []  # tuple: source, destination
        failure = []  # triple: source, destination, fail reason

        if not src.utils.create_dir(localcache):
            print_error(f"ERROR: Cannot create local cache {localcache}")
            sys.exit(1)

        
        for key, file in source_to_dest.items():
            print_message(f"STATUS: Fetch data from iRODS {key} --> {localcache}")
            size = src.irods_functions.get_irods_size(session, [key])
            
            # Determine size of source
            if size > cachelimit:
                print_warning(f"WARNING: Datasize exceeds cache size: {key}")
                failure.append((key, file, "Exceeds cache"))
                continue

            # create destination folder on remote server
            mkdir_remote = src.rsync.create_remote_dir(datauser, serverip, sudo, file)
            if not mkdir_remote:
                print_error(f"ERROR: mkdir on remote server failed {file}")
                failure.append((key, file, "Creating remote dir failed"))
                continue

            # irsync data to stepping stone
            irods_success = src.irods_functions.irsync_irods_to_local(session, key, localcache)
            if not irods_success:
                print_error(f"ERROR iRODS: transfer failed {key} {localcache}")
                failure.append((key, file, "iRODS transfer (irsync) failed"))
                continue

            # rsync data from stepping stone to destination server
            destination = file
            rsync_success = src.rsync.rsync_local_to_remote(
                    datauser, serverip, sudo, f"{localcache}/{os.path.basename(key)}", destination)
            if rsync_success:
                success.append((key, file))
                if session.collections.exists(key):
                    success.extend(src.irods_functions.map_collitems_to_local_path(session, key, destination))

                # Create iRODS metadata entry
                # print("DEBUG: annotate", key)
                # src.irods_functions.annotate_data(session, key, f"{destination}/{os.path.basename(key)}", serverip)
                # Delete cache
                src.rsync.empty_dir(localcache)
            else:
                print_error(f"ERROR rsync: transfer failed {localcache}/{os.path.basename(key)} "
                            +f"{os.path.dirname(destination)}")
                failure.append((key, file, "rsync to remote failed"))
                continue

        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        successpath = os.path.join(self.output_folder,
                                   f'output_irods_data_transfer_{timestamp}.csv')
        failurepath = os.path.join(self.output_folder,
                                   f'error_irods_data_transfer_{timestamp}.csv')

        src.utils.write_csv(success=success,
                            failure=failure,
                            successpath=successpath,
                            failurepath=failurepath)


if __name__ == "__main__":

    bridge = iBridgesSteppingStone.from_arguments()
    # or
    # bridge = iBridgesSteppingStone(input_csv='/data/ibridges/test.csv', transfer_config='...', output_folder='...')
    if bridge.operation == "export":
        bridge.export()
    elif bridge.operation == "import":
        print_message(f'Importing data into iRODS: TODO')
    else:
        print_error(f'Operation not defined: {bridge.operation}')

