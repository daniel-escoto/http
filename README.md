# http
HTTP server written in low-level C. Handles PUT and GET protocols. Implements multi-threading, file redundancy, and file backups.

## How to install
1. Open terminal and cd into folder.
2. In the terminal, run the following command: `make`.

## How to operate
### Basic use:
1. In terminal, run `./http <hostname> <portNumber>`.

### Enable redundancy:
1. In terminal, run `./http <hostname> <portNumber> -r`.

### Enable multithreading:
1. In terminal, run `./http <hostname> <portNumber> -N <threadCount>`.

### Backup:
1. Send `curl http://<hostname>:<portNumber>/b`

### Recover most recent backup:
1. Send `curl http://<hostname>:<portNumber>/r`

### Show list of available backups:
1. Send `curl http://<hostname>:<portNumber>/l`
