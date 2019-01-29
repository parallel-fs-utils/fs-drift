#!/usr/bin/python2

# fs-drift.py - run a mixed, multi-thread, multi-host workload
# on a POSIX filesystem

# all the heavy lifting is done in "invocation" module,
# how to run:
#
# ./fs-drift.py
# 
# how to get help:
#
# ./fs-drift.py -h
#
# documentation at https://github.com/parallel-fs-utils/fs-drift
#

'''
Copyright 2015 -- Ben England
Licensed under the Apache License at http://www.apache.org/licenses/LICENSE-2.0
See Appendix on this page for instructions pertaining to license.
'''

import os
import os.path
import time
import sys
import random
import event
import errno
import pickle

import common
from common import rq, OK, NOTOK
from common import ensure_deleted, FsDriftException
import opts
import fsd_log
import output_results
import ssh_thread
import launcher_thread
import sync_files
import output_results
import multi_thread_workload
from sync_files import write_pickle, read_pickle

def abort_test(prm):
    multi_thread_workload.abort_test(prm.abort_path, remote_thread_list)
    sys.exit(NOTOK)

# run a multi-host test

def run_multi_host_workload(prm, log):

    # construct list of ssh threads to invoke in parallel

    if os.getenv('PYPY'):
        python_prog = os.getenv('PYPY')
    elif sys.version.startswith('2'):
        python_prog = 'python'
    elif sys.version.startswith('3'):
        python_prog = 'python3'
    else:
        raise Exception('unrecognized python version %s' % sys.version)

    log.debug('python_prog = %s'%python_prog)

    remote_thread_list = []
    host_ct = len(prm.host_set)
    for j in range(0, len(prm.host_set)):
        remote_host = prm.host_set[j]
        fsd_remote_pgm = os.path.join(prm.fsd_remote_dir,
                                      'fs-drift-remote.py')
        this_remote_cmd = '%s %s --network-sync-dir %s ' \
            % (prm.python_prog, fsd_remote_pgm, prm.network_shared_path)

        this_remote_cmd += ' --as-host %s' % remote_host
        log.debug(this_remote_cmd)
        if prm.launch_as_daemon:
            remote_thread_list.append(
                launcher_thread.launcher_thread(prm, log, remote_host, this_remote_cmd))
        else:
            remote_thread_list.append(
                ssh_thread.ssh_thread(log, remote_host, this_remote_cmd))

    # start them, pacing starts so that we don't get ssh errors

    for t in remote_thread_list:
        if prm.launch_as_daemon:
            time.sleep(0.1)
        t.start()

    # wait for hosts to arrive at starting gate
    # if only one host, then no wait will occur
    # as starting gate file is already present
    # every second we resume scan from last host file not found

    exception_seen = None
    abortfn = prm.abort_path
    sec_delta = 0.5
    # timeout if no host replies in next host_timeout seconds
    per_host_timeout = 10.0
    all_host_timeout = 5.0 + len(prm.host_set) / 3
    if all_host_timeout < per_host_timeout:
        per_host_timeout = all_host_timeout / 2

    hosts_ready = False  # set scope outside while loop
    last_host_seen = -1
    sec = 0.0
    start_loop_start = time.time()
    try:
        while sec < per_host_timeout:
            # HACK to force directory entry coherency for Gluster
            #ndirlist = os.listdir(prm.network_shared_path)
            #log.debug('shared dir list: ' + str(ndirlist))
            hosts_ready = True
            if os.path.exists(abortfn):
                raise FsDriftException('worker host signaled abort')
            for j in range(last_host_seen + 1, len(prm.host_set)):
                h = prm.host_set[j]
                fn = multi_thread_workload.gen_host_ready_fname(prm, h.strip())
                log.debug('checking for host filename ' + fn)
                if not os.path.exists(fn):
                    log.info('did not see host filename %s after %f sec' % (fn, sec))
                    hosts_ready = False
                    break
                log.debug('saw host filename ' + fn)
                last_host_seen = j  # saw this host's ready file
                # we exit while loop only if no hosts in per_host_timeout seconds
                sec = 0.0
            if hosts_ready:
                break

            # if one of ssh threads has died, no reason to continue

            kill_remaining_threads = False
            for t in remote_thread_list:
                if not t.isAlive():
                    log.error('thread %s has died' % t)
                    kill_remaining_threads = True
                    break
            if kill_remaining_threads:
                break

            # be patient for large tests
            # give user some feedback about
            # how many hosts have arrived at the starting gate

            time.sleep(sec_delta)
            sec += sec_delta
            time_since_loop_start = time.time() - start_loop_start
            log.debug('last_host_seen=%d sec=%d' % (last_host_seen, sec))
            if time_since_loop_start > all_host_timeout:
                kill_remaining_threads = True
                break
    except KeyboardInterrupt as e:
        log.error('saw SIGINT signal, aborting test')
        exception_seen = e
    except Exception as e:
        exception_seen = e
        log.exception(e)
        hosts_ready = False
    if not hosts_ready:
        multi_thread_workload.abort_test(prm.abort_path, remote_thread_list)
        if not exception_seen:
            log.info(
                'no additional hosts reached starting gate within %5.1f seconds' % per_host_timeout)
            return NOTOK
        else:
            raise exception_seen
    else:

        # ask all hosts to start the test
        # this is like firing the gun at the track meet

        try:
            sync_files.write_sync_file(prm.starting_gun_path, 'hi')
            log.debug('starting all threads by creating starting gun file %s' %
                        prm.starting_gun_path)
        except IOError as e:
            log.error('error writing starting gun file: %s' % os.strerror(e.errno))
            multi_thread_workload.abort_test(prm.abort_path, remote_thread_list)
            raise e
            
    # wait for them to finish

    for t in remote_thread_list:
        t.join()
        if t.status != OK:
            log.error('ssh thread for host %s completed with status %d' %
                      (t.remote_host, t.status))

    # attempt to aggregate results by reading pickle files
    # containing SmallfileWorkload instances
    # with counters and times that we need

    try:
        invoke_list = []
        one_shot_delay = True
        for h in prm.host_set:  # for each host in test

            # read results for each thread run in that host
            # from python pickle of the list of SmallfileWorkload objects

            pickle_fn = multi_thread_workload.host_result_filename(prm, h)
            log.debug('reading pickle file: %s' % pickle_fn)
            host_invoke_list = []
            try:
                if one_shot_delay and not os.path.exists(pickle_fn):

                    # all threads have joined already, they are done
                    # we allow > 1 sec
                    # for this (NFS) client to see other clients' files

                    time.sleep(1.2)
                    one_shot_delay = False
                host_invoke_list = read_pickle(pickle_fn)
                log.debug(' read %d invoke objects' % len(host_invoke_list))
                invoke_list.extend(host_invoke_list)
                ensure_deleted(pickle_fn)
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise e
                log.error('  pickle file %s not found' % pickle_fn)

        output_results.output_results(prm, invoke_list)
    except IOError as e:
        log.exception(e)
        log.error('host %s filename %s: %s' % (h, pickle_fn, str(e)))
        return NOTOK
    except KeyboardInterrupt as e:
        log.error('control-C signal seen (SIGINT)')
        return NOTOK
    except FsDriftException as e:
        log.exception(e)
        return NOTOK
    return(OK)


# main routine that does everything for this workload

def run_workload():

    log = fsd_log.start_log('fs-drift')

    # if a --host-set parameter was passed,
    # it's a multi-host workload
    # each remote instance will wait
    # until all instances have reached starting gate

    try:
        params = opts.parseopts()
        params.validate()
    except FsDriftException as e:
        log.error(str(e))
        log.info('use --help option to get CLI syntax')
        sys.exit(NOTOK)

    print(params)

    if os.getenv('DEBUG'):
        log.logLevel(logging.DEBUG)

    try:
        sync_files.create_top_dirs(params)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise FsDriftException(
                'you must create the top-level directory %s' % 
                params.top_directory)

    # put parameters where all threads can see them

    write_pickle(params.param_pickle_path, params)

    if params.host_set != [] and not params.is_slave:
        return run_multi_host_workload(params, log)
    return multi_thread_workload.run_multi_thread_workload(params)


# for future windows compatibility,
# all global code (not contained in a class or subroutine)
# must be moved to within a routine unless it's trivial (like constants)
# because windows doesn't support fork().

if __name__ == '__main__':
    sys.exit(run_workload())
