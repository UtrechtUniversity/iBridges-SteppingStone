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

        config = src.utils.get_config(configfile=self.transfer_config)
        if config:
            self.datauser, self.serverip, self.sudo, self.cachelimit = config

        self.run()

    @classmethod
    def from_arguments(cls):
        parser = argparse.ArgumentParser(
            prog='python transfer_workflow.py',
            description='Transfers data between Yoda/iRODS and a destination server through '
                        + 'a stepping stone server',
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
                            help='export (iRODS/YODA to remote server, import (remote server to iRODS/YODA)',
                            required=True)

        args = parser.parse_args()

        return cls(
            input_csv=args.input,
            output_folder=args.output,
            transfer_config=args.config,
            irods_env_file=args.env,
            operation=args.operation)

    def run(self):
        if self.operation == "export":
            self.exportData()
        elif self.operation == "import":
            self.importData()
        else:
            print_error(f'Operation not defined: {self.operation}')

    def setup_transfer(self, source_to_dest):
        # Initial check on csv file
        if len(source_to_dest) == 0:
            print_error("Nothing to transfer")
            print_message("Empty file, or not a CSV-file")
            sys.exit(1)

        # Check ssh connection and auth
        if not src.rsync.ssh_check_connection(self.datauser, self.serverip):
            return None

        # Create iRODS session
        irods_conn = src.irods_functions.init_irods_connection(irods_env_file=self.irods_env_file)
        if irods_conn:
            session, _ = irods_conn
        else:
            return None

        localcache = os.getenv('HOME') + "/irodscache"
        success = []  # tuple: source, destination
        failure = []  # triple: source, destination, fail reason

        return session, localcache, success, failure

    def write_log(self, success, failure):
        timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
        successpath = os.path.join(self.output_folder,
                                   f'output_irods_data_transfer_{timestamp}.csv')
        failurepath = os.path.join(self.output_folder,
                                   f'error_irods_data_transfer_{timestamp}.csv')
        src.utils.write_csv(success=success,
                            failure=failure,
                            successpath=successpath,
                            failurepath=failurepath)

    def create_local_cache(self, localcache):
        if not src.utils.create_dir(localcache):
            print_error(f"ERROR: Cannot create local cache {localcache}")
            sys.exit(1)

    def importData(self):
        source_to_dest = src.utils.read_source_dest_csv(filename=self.input_csv)
        setup = self.setup_transfer(source_to_dest)
        if setup:
            session, localcache, success, failure = setup
        else:
            sys.exit(1)

        # Check if remote paths exist
        for key in list(source_to_dest.keys()):
            if not src.rsync.remote_path_exists(self.datauser, self.serverip, key):
                print_warning(f"WARNING: Remote path does not exist: {key}")
                del source_to_dest[key]
        if len(source_to_dest) == 0:
            print_error("Nothing to transfer, check CSV-file")
            sys.exit(1)

        self.create_local_cache(localcache)

        # Copy data remote --> localcache --> irods
        for key, value in source_to_dest.items():
            print_message(f"STATUS: Fetch data from remote server {key} --> {localcache}")
            size = src.rsync.get_remote_size(self.datauser, self.serverip, [key])

            if size > self.cachelimit:
                print_warning(f"WARNING: Datasize exceeds cache size: {key}")
                failure.append((key, value, "Exceeds cache"))
                continue

            # Create iRODS collection
            if not src.irods_functions.ensure_coll(session, value):
                print_warning(f"WARNING: Skipping: {key, value}")
                failure.append((key, value, "Destination could not be created"))
                continue

            # rsync to stepping stone
            rsync_success = src.rsync.rsync_remote_to_local(self.datauser, self.serverip,
                                                            self.sudo, key, localcache)
            if not rsync_success:
                print_warning(f"WARNING: Remote to cache failed: {key, value}")
                failure.append((key, value, "rsync remote to local failed"))
                src.rsync.empty_dir(localcache)

            # irsync to iRODS
            item_name = os.path.basename(key)
            irods_success = src.irods_functions.irsync_local_to_irods(
                    session, localcache + '/' + item_name, value)
            if irods_success:
                print_message("--> Data transfer complete")
                success.append((key, f'{value}/{os.path.basename(key)}'))
                if session.collections.exists(f'{value}/{os.path.basename(key)}'):
                    success.extend(src.irods_functions.map_collitems_to_folder(
                        session, f'{value}/{os.path.basename(key)}', key, True))
                src.rsync.empty_dir(localcache)
            else:
                print_warning(f"WARNING: Local to iRODS failed: {key, value}")
                failure.append((key, value, "irsync local to iRODS failed"))
                src.rsync.empty_dir(localcache)
                continue

        self.write_log(success, failure)

    def exportData(self):
        source_to_dest = src.utils.read_source_dest_csv(filename=self.input_csv)
        setup = self.setup_transfer(source_to_dest)
        if setup:
            session, localcache, success, failure = setup
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

        self.create_local_cache(localcache)

        for key, value in source_to_dest.items():
            print_message(f"STATUS: Fetch data from iRODS {key} --> {localcache}")
            size = src.irods_functions.get_irods_size(session, [key])

            # Determine size of source
            if size > self.cachelimit:
                print_warning(f"WARNING: Datasize exceeds cache size: {key}")
                failure.append((key, value, "Exceeds cache"))
                continue

            # create destination folder on remote server
            mkdir_remote = src.rsync.create_remote_dir(self.datauser, self.serverip, 
                                                       self.sudo, value)
            if not mkdir_remote:
                print_error(f"ERROR: mkdir on remote server failed {value}")
                failure.append((key, value, "Creating remote dir failed"))
                continue

            # irsync data to stepping stone
            irods_success = src.irods_functions.irsync_irods_to_local(session, key, localcache)
            if not irods_success:
                print_error(f"ERROR iRODS: transfer failed {key} {localcache}")
                failure.append((key, value, "iRODS transfer (irsync) failed"))
                src.rsync.empty_dir(localcache)
                continue

            # rsync data from stepping stone to destination server
            rsync_success = src.rsync.rsync_local_to_remote(
                    self.datauser, self.serverip, self.sudo, 
                    f"{localcache}/{os.path.basename(key)}", value)
            if rsync_success:
                print_message("--> Data transfer complete")
                success.append((key, f"{value}/{os.path.basename(key)}"))
                if session.collections.exists(key):
                    success.extend(src.irods_functions.map_collitems_to_folder(session, key, value))

                # Create iRODS metadata entry
                # print("DEBUG: annotate", key)
                # src.irods_functions.annotate_data(session, key,
                #                               f"{destination}/{os.path.basename(key)}", serverip)
                # Delete cache
                src.rsync.empty_dir(localcache)
            else:
                print_error(f"ERROR rsync: transfer failed {localcache}/{os.path.basename(key)} "
                            + f"{os.path.dirname(value)}")
                failure.append((key, value, "rsync to remote failed"))
                src.rsync.empty_dir(localcache)
                continue

        self.write_log(success, failure)


if __name__ == "__main__":

    bridge = iBridgesSteppingStone.from_arguments()
    # or
    # bridge = iBridgesSteppingStone(input_csv='/data/ibridges/test.csv',
    #                                transfer_config='...', output_folder='...')
