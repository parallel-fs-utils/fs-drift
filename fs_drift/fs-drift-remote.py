#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Copyright 2012 -- Ben England
Licensed under the Apache License at http://www.apache.org/licenses/LICENSE-2.0
See Appendix on this page for instructions pertaining to license.
'''

import sys
import os
import errno
import time
import pickle
import argparse
import socket

import fs_drift.multi_thread_workload
import fs_drift.common
from fs_drift.sync_files import read_pickle

# parse command line and return unpickled test params
# pass via --network-sync-dir option
# optionally pass host identity of this remote invocation


def parse():

    parser = argparse.ArgumentParser(
            description='parse remote fs-drift parameters')
    parser.add_argument('--network-sync-dir',
                        help='directory used to synchronize with test driver')
    parser.add_argument('--as-host',
                        default=socket.gethostname(),
                        help='directory used to synchronize with test driver')
    args = parser.parse_args()
    if args.network_sync_dir == None:
        raise fs_drift.common.FsDriftException('you must specify --network-sync-dir')
    param_pickle_fname = os.path.join(args.network_sync_dir, 'params.pickle')
    if not os.path.exists(param_pickle_fname):
        time.sleep(1.1)
    params = read_pickle(param_pickle_fname)
    params.is_slave = True
    params.as_host = args.as_host
    return params


# main routine that does everything for this workload

def run_workload():

    # if a --host-set parameter was passed, it's a multi-host workload
    # each remote instance will wait until all instances reach starting gate

    params = parse()
    return fs_drift.multi_thread_workload.run_multi_thread_workload(params)


# for windows compatibility,
# all global code (not contained in a class or subroutine)
# must be moved to within a routine unless it's trivial (like constants)
# because windows doesn't support fork().

if __name__ == '__main__':
    run_workload()
