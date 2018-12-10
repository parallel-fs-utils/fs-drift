#!/usr/bin/python
# -*- coding: utf-8 -*-
import os
import sys
import time
import random
import copy
import socket

import worker_thread
import common
from common import OK, NOTOK, FsDriftException, ensure_deleted
import fsd_log
import invoke_process
import sync_files
import output_results

def create_worker_list(prm):

    # for each thread set up FsDriftWorkload instance,
    # create a thread instance, and delete the thread-ready file

    thread_list = []
    for k in range(0, prm.threads):
        nextinv = worker_thread.FsDriftWorkload(prm)
        nextinv.tid = '%02d' % k
        t = invoke_process.subprocess(nextinv)
        thread_list.append(t)
        ensure_deleted(nextinv.gen_thread_ready_fname(nextinv.tid))
    return thread_list


# abort routine just cleans up threads
# and makes sure that any running threads 
# on remote hosts will terminate by setting abort flag file

def abort_test(abort_fn, thread_list):
    if not os.path.exists(abort_fn):
        common.touch(abort_fn)
    for t in thread_list:
        t.terminate()


# each host uses this to signal that it is
# ready to immediately begin generating workload
# each host places this file in a directory shared by all hosts
# to indicate that this host is ready

def gen_host_ready_fname(params, hostname):
    return os.path.join(params.network_shared_path, 'host_ready.' + hostname + '.tmp')


# file for result stored as pickled python object

def host_result_filename(params, result_host):
    return os.path.join(params.network_shared_path, result_host + '_result.pickle')


# what follows is code that gets done on each host

def run_multi_thread_workload(prm):

    host = prm.as_host
    if host == None:
        host = 'localhost'
    prm_slave = (prm.host_set != [])
    # FIXME: get coherent logging level interface
    verbose = os.getenv('LOGLEVEL_DEBUG' != None)
    host_startup_timeout = 5  + len(prm.host_set) / 3

    # for each thread set up SmallfileWorkload instance,
    # create a thread instance, and delete the thread-ready file

    thread_list = create_worker_list(prm)
    my_host_invoke = thread_list[0].invoke
    my_log = fsd_log.start_log('%s.master' % host)
    my_log.debug(prm)

    # start threads, wait for them to reach starting gate
    # to do this, look for thread-ready files

    for t in thread_list:
        ensure_deleted(t.invoke.gen_thread_ready_fname(t.invoke.tid))
    for t in thread_list:
        t.start()
    my_log.debug('started %d worker threads on host %s' %
                                (len(thread_list), host))

    # wait for all threads to reach the starting gate
    # this makes it more likely that they will start simultaneously

    abort_fname = prm.abort_path
    thread_count = len(thread_list)
    thread_to_wait_for = 0
    startup_timeout = 3
    sec = 0.0
    while sec < startup_timeout:
        for k in range(thread_to_wait_for, thread_count):
            t = thread_list[k]
            fn = t.invoke.gen_thread_ready_fname(t.invoke.tid)
            if not os.path.exists(fn):
                my_log.debug('thread %d thread-ready file %s not found yet with %f sec left' % 
                            (k, fn, (startup_timeout - sec)))
                break
            thread_to_wait_for = k + 1
            # we only timeout if no more threads have reached starting gate
            # in startup_timeout sec
            sec = 0.0
        if thread_to_wait_for == thread_count:
            break
        if os.path.exists(abort_fname):
            break
        sec += 0.5
        time.sleep(0.5)

    # if all threads didn't make it to the starting gate

    if thread_to_wait_for < thread_count:
        abort_test(abort_fname, thread_list)
        raise FsDriftException('only %d threads reached starting gate' 
                                % thread_to_wait_for)

    # declare that this host is at the starting gate

    if prm_slave:
        host_ready_fn = gen_host_ready_fname(prm, prm.as_host)
        my_log.debug('host %s creating ready file %s' %
                     (my_host_invoke.onhost, host_ready_fn))
        common.touch(host_ready_fn)

    sg = prm.starting_gun_path
    if not prm_slave:
        my_log.debug('wrote starting gate file ')
        sync_files.write_sync_file(sg, 'hi there')

    # wait for starting_gate file to be created by test driver
    # every second we resume scan from last host file not found

    if prm_slave:
        my_log.debug('awaiting ' + sg)
        for sec in range(0, host_startup_timeout+3):
            # hack to ensure that directory is up to date
            #   ndlist = os.listdir(my_host_invoke.network_dir)
            # if verbose: print(str(ndlist))
            if os.path.exists(sg):
                break
            if os.path.exists(prm.abort_path):
                log.info('saw abort file %s, aborting test' % prm.abort_path)
                break
            time.sleep(1)
        if not os.path.exists(sg):
            abort_test(prm.abort_path, thread_list)
            raise Exception('starting signal not seen within %d seconds'
                            % host_startup_timeout)
    if verbose:
        print('starting test on host ' + host + ' in 2 seconds')
    time.sleep(2 + random.random())  # let other hosts see starting gate file

    # FIXME: don't timeout the test,
    # instead check thread progress and abort if you see any of them stalled
    # but if servers are heavily loaded you can't rely on filesystem

    # wait for all threads on this host to finish

    for t in thread_list:
        my_log.debug('waiting for thread %s' % t.invoke.tid)
        t.retrieve()
        t.join()

    # if not a slave of some other host, print results (for this host)

    if not prm_slave:
        try:
            worker_list = [ t.invoke for t in thread_list ] 
            output_results.output_results(prm, worker_list)
        except FsDriftException as e:
            print('ERROR: ' + str(e))
            return NOTOK
    else:

        # if we are participating in a multi-host test
        # then write out this host's result in pickle format
        # so test driver can pick up result

        result_filename = host_result_filename(prm, prm.as_host)
        my_log.debug('saving result to filename %s' % result_filename)
        worker_list = [ t.invoke for t in thread_list ]
        sync_files.write_pickle(result_filename, worker_list)
        time.sleep(1.2)  # for benefit of NFS with actimeo=1

    return OK
