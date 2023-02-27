# iBridges-SteppingStone
Transferring data from Yoda/iRODS to a destination server through a stepping stone server in the middle.

## Use case
For sensitive data sometimes the iRODS instance is protected by firewalls such that one cannot directly transfer data from the iRODS server to the compute site (Destination server).

To bridge the firewall, usually a stepping stone server is setup at the p[eriphery of the network. This server can connect to iRODS and also to the compute facility.
Hence there are two copy steps involved. Here we chose for `irsync` to transger the data from iRODS to the stepping stone server and `rsync` to copy the data from the stepping stone server to the final destination, e.g. the compute server or VM (see below).

![Stepping stone transfer](img/Stepping_stone.png)

In this use case we assume that

1. Both servers, stepping stone and destination server, are *linux* servers
2. The user which transfers data between the Stepping Stine server and the destination server authenticates through an ssh-key pair.
3. The iRODS `icommands` are installed on the Stepping stone server.

## Requirements
- SSH keypair to authenticate between stepping stone server and destination server
- `icommands` are installed on stepping stone server
- Python dependencies on stepping stone server:
	- Python 3.X
	- python-irodsclient version 1.X

The scripts will be executed on the stepping stone server.

**The scripts are tested on Ubuntu with python 3.6.9.**

## Installation & configuration

- iRODS: a valid iRODS configuration file in `~/.irods/irods_environment.json`
- Python dependencies: `pip3 install python-irodsclient==1.1.6`
- Client configuration:
	- The client needs to be given the information which destination server to copy data to and which user to use for the actions.
	- Configuration file in `~/.irods/transfer.config`
	
	```sh
	[remote]
    serverip: IP address or FQDN
    datauser: user
    sudo: False

    [local_cache]
    limit = number of GB of free space on stepping stone server, only the number
    ```

## Usage
```
Usage: python3 transfer_workflow.py -i, --input=csv-file-path
Example: python3 transfer_workflow.py -i /home/user/transfer.csv
```
	
