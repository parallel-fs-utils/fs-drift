#!/usr/bin/env python
# -*- coding: utf-8 -*-
# launch_daemon.py
# background process which waits for a request to run a fs-drift-remote.py command
# This handles 2 use cases:
#   - running windows or other non-linux OS.
#   - containers, which typically don't come with sshd
#
# Windows:
# we substitute --top directory with --substitute_top directory
# so that Windows clients can run with Linux test drivers,
# which cannot have the same pathname for the shared directory
# as the Windows clients, so you don't need to specify
# --substitute_top in any other situation.
#
# Example for Windows:
# if mountpoint on Linux test driver is /mnt/cifs/testshare
# and mountpoint on Windows is z:\
# you run:
#   python launch_smf_host.py \
#              --top /mnt/cifs/testshare/smf
#              --substitute_top z:\smf
#
# Containers:
# if you are doing all Linux with containers, then
# you need to specify container ID in Docker startup command
# if your mountpoint for the shared storage is /mnt/fs:
#   CMD: python launch_daemon.py --top $top_dir --as-host container$container_id
#
# for example, you could include this as the last line in your Dockerfile
# and fill in top_dir and container_id as environment variables in
# your docker run command using the -e option
# # docker run -e top_dir=/mnt/fs/smf -e container_id="container-2"
#
#
import sys
import os
import time
import errno
import logging
import socket
import argparse

from fs_drift.fsd_log import start_log

OK = 0
NOTOK = 1

verbose = (os.getenv("VERBOSE") != None)
log = start_log('launcher', verbosity=verbose)


def myabort(msg):
    log.error(msg)
    sys.exit(NOTOK)


substitute_dir = None
top_dir = None
# get short hostname
as_host = socket.gethostname().split('.')[0]

parser = argparse.ArgumentParser(description='parse fs-drift/launch_daemon.py parameters')
a = parser.add_argument
a('--top', help='top-level shared filesystem directory for fs-drift')
a('--substitute-top', default=substitute_dir, help='replace --top with this directory')
a('--as-host', default=as_host, help='hostname/container-ID that this daemon launches')
args = parser.parse_args()

top_dir = args.top
as_host = args.as_host
substitute_dir = args.substitute_top

log.info('substitute-top %s, top directory %s, as-host %s' %
         (substitute_dir, top_dir, as_host))

if top_dir == None:
    myabort('you must define --top parameter')

# look for launch files, read command from them,
# and execute, substituting --shared directory for --top directory,
# to allow samba to work with Linux test driver

network_shared_path = os.path.join(top_dir, 'network-shared')
launch_fn = os.path.join(network_shared_path, as_host) + '.fsd_launch'
if os.path.exists(launch_fn):  # avoid left-over launch files
    os.unlink(launch_fn)
log.info('launch filename ' + launch_fn)

launcher_kill_fn = os.path.join(network_shared_path, 'stop_launcher')
if os.path.exists(launcher_kill_fn):
    os.unlink(launcher_kill_fn)
log.info('pathname to stop launcher: %s' % launcher_kill_fn)

while True:
    try:
        with open(launch_fn, 'r') as f:
            cmd = f.readline().strip()
        os.unlink(launch_fn)
        if substitute_dir != None:
            cmd = cmd.replace(substitute_dir, top_dir)
        log.debug('spawning cmd: %s' % cmd)
        rc = os.system(cmd)
        if rc != OK:
            log.debug('ERROR: return code %d for cmd %s' % (rc, cmd))
    except IOError as e:
        if e.errno != errno.ENOENT:
            raise e
    finally:
        time.sleep(1)
    if os.path.exists(launcher_kill_fn):
        log.info('saw %s, exiting as requested' % launcher_kill_fn)
        sys.exit(OK)
