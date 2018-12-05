#!/usr/bin/env python
# -*- coding: utf-8 -*-


'''
worker_thread.py -- FsDriftWorkload class used in each workload thread
Licensed under the Apache License at http://www.apache.org/licenses/LICENSE-2.0
See Appendix on this page for instructions pertaining to license.
'''

# allow for multi-thread tests
# allow threads to stop as soon as enough other threads finish
# we can launch any combination of these to simulate more complex workloads
# to run just one of unit tests do
#   python -m unittest smallfile.Test.your-unit-test
# unittest2 is there for backwards compatibility
# so it now uses unittest2,
# but it isn't installed by default so we have to conditionalize its use
# we only need it installed where we want to run regression test this way
# on Fedora:
#   yum install python-unittest2
# alternative single-test syntax:
#   python worker_thread.py -v Test.test_c1_Mkdir

import os
import os.path
from os.path import exists, join
import sys
import time
import copy
import random
import logging
import threading
import socket
import errno
import codecs

# fs-drift modules
import common
from common import touch, FsDriftException, FileSizeDistr, FileAccessDistr
from common import ensure_dir_exists, deltree, OK
import event
import fsop
import fsd_log
from sync_files import write_pickle, read_pickle

# process exit status for success and failure
OK = 0
NOTOK = 1

# unit conversions
KB_PER_GB = 1 << 20
BYTES_PER_KB = 1024
MICROSEC_PER_SEC = 1000000.0


class FsDriftWorkload:

    rename_suffix = '.rnm'

    # number of files between threads-finished check at smallest file size
    max_files_between_checks = 100

    # multiply mean size by this to get max file size

    random_size_limit = 8

    # large prime number used to randomly select directory given file number

    some_prime = 900593

    # build largest supported buffer, and fill it full of random hex digits,
    # then just use a substring of it below

    biggest_buf_size_bits = 20
    random_seg_size_bits = 10
    biggest_buf_size = 1 << biggest_buf_size_bits

    # initialize files with up to this many different random patterns
    buf_offset_range = 1 << 10

    # constructor sets up initial, default values for test parameters
    # user overrides these values using CLI interface parameters
    # for boolean parameters,
    # preceding comment describes what happens if parameter is set to True

    def __init__(self, params):

        self.params = params
        self.ctx = None
        self.ctrs = fsop.FSOPCounters()

        # total_threads is thread count across entire distributed test
        # FIXME: take into account thread count and multiple hosts running threads
        self.total_threads = 0

        # test-over polling rate
        self.files_between_checks = 20

        self.tmp_dir = '/var/tmp'
        e = os.getenv('TMPDIR')
        if e:
            self.tmp_dir = e

        # which host the invocation ran on
        self.onhost = socket.gethostname()

        # thread ID - let caller fill this in
        self.tid = ''

        # debug to screen
        self.log_to_stderr = False

        # logging level, default is just informational, warning or error
        self.log_level = logging.INFO

        # will be initialized later with thread-safe python logging object
        self.log = None

        # buffer for reads and writes will be here
        self.buf = None

        # copy from here on writes, compare to here on reads
        self.biggest_buf = None

        # random seed used to control sequence of random numbers,
        # default to different sequence every time
        self.randstate = random.Random()

        self.total_threads = len(self.params.host_set) * self.params.threads

        # reset object state variables

        self.reset()

    # convert object to string for logging, etc.

    # if you want to use the same instance for multiple tests
    # call reset() method between tests

    def reset(self):

        # results returned in variables below
        self.abort = False
        self.status = OK

        # every few seconds we see if there is a verbosity file 
        # in network_shared_path, and if it has changed, if it has
        # then we reload verbosity from this file
        # so you can turn on/off debug logging in different areas 
        # in the middle of a run
        self.verbosity = 0
        self.verbosity_last_checked = 0
        self.verbosity_poll_rate = 1
        self.pause_sec = self.params.pause_between_ops / MICROSEC_PER_SEC

        # to measure per-thread elapsed time
        self.end_time = 0.0
        self.start_time = 0.0
        self.elapsed_time = 0.0
        # to measure file operation response times
        self.op_start_time = None
        self.rsptimes = []


    # create per-thread log file
    # we have to avoid getting the logger for self.tid more than once,
    # or else we'll add a handler more than once to this logger
    # and cause duplicate log messages in per-invoke log file

    def start_log(self):
        self.log = logging.getLogger(self.tid)
        if self.log_to_stderr:
            h = logging.StreamHandler()
        else:
            h = logging.FileHandler(self.log_fn())
        log_format = (' %(asctime)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter(log_format)
        h.setFormatter(formatter)
        self.log.addHandler(h)
        self.loglevel = logging.INFO
        self.log.setLevel(self.loglevel)
        self.log.info('starting log')

    # update verbosity if necessary

    def update_verbosity(self):
        # only check if it hasn't been checked in the last 10 sec
        now = time.time()
        if now - self.verbosity_last_checked < self.verbosity_poll_rate:
            return
        self.verbosity_last_checked = now
        vpath = os.path.join(self.params.network_shared_path, 'verbosity')
        v = 0
        try:
            with open(vpath, 'r') as vpath_f:
                vstr = vpath_f.readline().strip()
                if vstr.startswith('0x'):
                    v = int(vstr, 16)
                else:
                    v = int(vstr)
            if self.verbosity & 0x40000000:
                self.log.debug('read in verbosity 0x%x from %s' % (v, vpath))
            if v != self.verbosity:
                if v != 0:
                    self.log.setLevel(logging.DEBUG)
                else:
                    self.log.setLevel(logging.INFO)
                self.log.debug('changing verbosity from 0x%x to 0x%x' % (self.verbosity, v))
                self.verbosity = v
                if self.ctx is not None:
                    self.ctx.verbosity = v
        except ValueError:
            self.log.error('could not parse verbosity %s in file %s' % (vstr, vpath))
            os.unlink(vpath)
        except IOError as e:
            if e.errno != errno.ENOENT:
                self.log.exception(e)
                raise e

    # indicate start of an operation

    def op_starttime(self, starttime=None):
        if self.params.rsptimes:
            if not starttime:
                self.op_start_time = time.time()
            else:
                self.op_start_time = starttime

    # indicate end of an operation,
    # this appends the elapsed time of the operation to .rsptimes array

    def op_endtime(self, opname):
        if self.params.rsptimes:
            end_time = time.time()
            rsp_time = end_time - self.op_start_time
            self.rsptimes.append((opname, self.op_start_time, rsp_time))
            self.op_start_time = None

    # save response times seen by this thread

    def save_rsptimes(self):
        fname = self.params.rsptime_path % (self.onhost, self.tid)
        with open(fname, 'w') as f:
            for (opname, start_time, rsp_time) in self.rsptimes:
                # time granularity is microseconds, accuracy is less
                f.write('%8s, %9.6f, %9.6f\n' %
                        (opname, start_time - self.start_time, rsp_time))
            os.fsync(f.fileno())  # particularly for NFS this is needed
        self.log.info('response times saved in %s' % fname)

    # determine if test interval is over for this thread

    # each thread uses this to signal that it is at the starting gate
    # (i.e. it is ready to immediately begin generating workload)

    def gen_thread_ready_fname(self, tid, hostname=None):
        return join(self.tmp_dir, 'thread_ready.' + tid + '.tmp')

    # log file for this worker thread goes here

    def log_fn(self):
        return join(self.tmp_dir, 'fsd.%s.log' % self.tid)

    # we use the seed function to control per-thread random sequence
    # we want seed to be saved
    # so that operations subsequent to initial create will know
    # what file size is for thread T's file j without having to stat the file

    def init_random_seed(self):
        fn = self.gen_thread_ready_fname(self.tid,
                                         hostname=self.onhost) + '.seed'
        thread_seed = str(time.time())
        if not os.path.exists(fn):
            thread_seed = str(time.time()) + ' ' + self.tid
            with open(fn, 'w') as seedfile:
                seedfile.write(str(thread_seed))
                self.log.debug('write seed %s ' % thread_seed)
        else:
            with open(fn, 'r') as seedfile:
                thread_seed = seedfile.readlines()[0].strip()
                self.log.debug('read seed %s ' % thread_seed)
        self.randstate.seed(thread_seed)

    def get_next_file_size(self):
        next_size = self.total_sz_kb
        if self.params.filesize_distr == FileSizeDistr.exponential:
            next_size = max(1, min(int(self.randstate.expovariate(1.0
                            / self.total_sz_kb)), self.total_sz_kb
                            * self.random_size_limit))
            if self.log_level == logging.DEBUG:
                self.log.debug('rnd expn file size %d KB' % next_size)
        return next_size


    # tell test driver that we're at the starting gate
    # this is a 2 phase process
    # first wait for each thread on this host to reach starting gate
    # second, wait for each host in test to reach starting gate
    # in case we have a lot of threads/hosts, sleep 1 sec between polls
    # also, wait 2 sec after seeing starting gate to maximize probability
    # that other hosts will also see it at the same time

    def wait_for_gate(self):
        if self.params.starting_gun_path:
            gateReady = self.gen_thread_ready_fname(self.tid)
            touch(gateReady)
            while not os.path.exists(self.params.starting_gun_path):
                if os.path.exists(self.params.abort_path):
                    raise FsDriftException(
                        'thread ' + str(self.tid) + ' saw abort flag')
                time.sleep(0.3)
        # wait a little longer so that
        # other clients have time to see that gate exists
        # give everyone else a chance to see that start-file is there
        # it takes at least 1 second for NFS to invalidate cached metadata
        # with actimeo=1
        time.sleep(2)


    def thread_done_record(self):
        # must be fixed-length string so we can compute threads done from file size
        return '%012.6f %12s %60s\n' % (self.elapsed_time, self.tid, self.onhost)


    # record info needed to compute test statistics

    def end_test(self):
        self.end_time = time.time()
        self.elapsed_time = self.end_time - self.start_time
        if self.elapsed_time > self.params.duration:
            # must be fixed-length string so we can compute threads done from file size
            elapsed_time_str = self.thread_done_record()
            try:
                with open(self.params.checkerflag_path, 'a+') as chflg_f:
                    chflg_f.write(elapsed_time_str)
                    sz = os.fstat(chflg_f.fileno()).st_size
            except IOError:
                try:
                    sz = os.stat(self.params.checkerflag_path)
                except OSError:
                    sz = 0
            threads_done = sz / len(elapsed_time_str)

    def test_ended(self):
        return self.end_time > self.start_time

    # see if we should do one more file
    # to minimize overhead, do not check checkered_flag file before every iteration

    def do_another_file(self):
        if self.params.stop_when_thrds_done and self.filenum % self.files_between_checks == 0:
            if not self.test_ended():
                try:
                    sz = os.stat(self.params.checkerflag_path).st_size
                except OSError as e:
                    if e.errno != errno.ENOENT:
                        raise e
                    sz = 0
                record_len = len(self.thread_done_record())
                threads_done = sz / record_len
                return False

        # if user doesn't want to finish all requests and test has ended, stop

        if not self.finish_all_rq and self.test_ended():
            return False
        if self.elapsed_time >= self.duration:
            if not self.test_ended():
                self.end_test()
            return False
        if self.abort:
            raise FsDriftException(
                'thread ' + str(self.tid) + ' saw abort flag')
        if self.pause_sec > 0.0:
            time.sleep(self.pause_sec)
        return True

    def chk_status(self):
        if self.status != OK:
            raise FsDriftException(
                'test failed, check log file %s' % self.log_fn())

    def do_workload(self):

        self.start_log()
        #ensure_dir_exists(self.params.network_shared_path)
        self.params = read_pickle(self.params.param_pickle_path)
        self.init_random_seed()
        self.ctx = fsop.FSOPCtx(self.params, self.log, self.ctrs)
        # FIXME: worker_thread doesn't use this buf!
        #self.biggest_buf = self.create_biggest_buf(False)
        # retrieve params from pickle file so that 
        # remote workload generators can read them

        os.chdir(self.params.top_directory)

        op = 0
        rq_map = self.ctx.gen_rq_map()
        last_stat_time = time.time()
        last_drift_time = time.time()
        stop_file = self.params.stop_file_path

        self.wait_for_gate()

        self.start_time = time.time()
        event_count = 0
        total_errors = 0
        weights = event.parse_weights(self.params, rq_map)
        normalized_weights = event.normalize_weights(weights)

        try:
          while True:
            self.update_verbosity()

            # if there is pause file present, do nothing

            if event_count % 100 == 0 and os.path.isfile(self.params.pause_path):
                self.log.info('pausing test because %s seen' % self.params.pause_path)
                time.sleep(5)
                continue

            # every 1000 events, check for "stop file" that indicates test should end

            event_count += 1
            if (event_count % 1000 == 0) and os.access(self.params.stop_file_path, os.R_OK):
                break

            x = event.gen_event(normalized_weights)
            (fn, name) = rq_map[x]
            if common.verbosity & 0x1:
                self.log.debug('event %s name %s' % (x, name))
            self.op_starttime()
            rc = fn()
            self.op_endtime(name)
            if rc != OK:
                self.log.debug("%s returns %d" % (name, rc))
                total_errors += 1

            # periodically output counters

            if ( (self.params.stats_report_interval > 0) and 
                 (before - last_stat_time > params.stats_report_interval)):
                output_results.print_stats(self.start_time, total_errors)
            last_stat_time = self.start_time

            # if using moving gaussian file access pattern...

            if (self.params.drift_time > 0):
                self.ctx.add_to_simulated_time(self.params.drift_time)

            # use duration to limit test

            if self.params.duration > 0:
                elapsed = time.time() - self.start_time
            if elapsed > self.params.duration:
                break

          if total_errors > 0:
            self.log.error('total of %d unexpected errors seen' % total_errors)
            self.status = NOTOK
        except KeyboardInterrupt as e:
            self.log.error('control-C (SIGINT) signal received, ending test')
            self.status = NOTOK
        except OSError as e:
            self.status = e.errno
            self.log.exception(e)
        except Exception as e:
            self.log.exception(e)
            self.status = -NOTOK
        self.end_test()
        if self.params.rsptimes:
            self.save_rsptimes()
        if self.status != OK:
            self.log.error('invocation did not complete cleanly')
        self.log.info('worker_thread do_workload finished')
        return self.status

# below are unit tests for SmallfileWorkload
# including multi-threaded test
# this should be designed to run without any user intervention
# to run just one of these tests do
#   python -m unittest2 smallfile.Test.your-unit-test

# so you can just do "python worker_thread.py " to test it

if __name__ == '__main__':
    import unittest2
    import opts

    # threads used to do multi-threaded unit testing
    
    class TestThread(threading.Thread):
    
        def __init__(self, my_worker, my_name):
            threading.Thread.__init__(self, name=my_name)
            self.worker = my_worker
            self.worker.tid = my_name
            self.worker.verbose = True

        def __str__(self):
            return 'TestThread ' + str(self.worker) + ' ' + \
                threading.Thread.__str__(self)
    
        def run(self):
            try:
                self.worker.do_workload()
            except Exception as e:
                self.worker.log.error(str(e))

    class Test(unittest2.TestCase):
        workload_table = [
                    'read, 2',
                    'random_read, 1',
                    'random_write, 1',
                    'append, 4',
                    'delete, 0.1',
                    'hardlink, 0.01',
                    'softlink, 0.02',
                    'truncate, 0.05',
                    'rename, 1',
                    'create, 4']
    
        def setUp(self):
            with open('/tmp/weights.csv', 'w') as w_f:
                w_f.write( '\n'.join(Test.workload_table))
            self.params = opts.parseopts()
            self.params.duration = 3
            self.params.workload_table_csv_path = '/tmp/weights.csv'
    
        def file_size(self, fn):
            st = os.stat(fn)
            return st.st_size

        def cleanup_files(self):
            sys.stderr.flush()
            sys.stdout.flush()
            deltree(self.params.network_shared_path)
            deltree(self.params.top_directory)
            ensure_dir_exists(self.params.top_directory)
            ensure_dir_exists(self.params.network_shared_path)
    
        def test_a_runthread(self):
            self.cleanup_files()

            write_pickle(self.params.param_pickle_path, self.params)
            fsd = FsDriftWorkload(self.params)
            fsd.tid = 'worker_thread'
            fsd.verbose = True
            touch(fsd.params.starting_gun_path)
            fsd.do_workload()
            print(fsd.ctrs)
            fsd.chk_status()

        def test_b_run2threads(self):
            self.cleanup_files()
            write_pickle(self.params.param_pickle_path, self.params)
            t1 = TestThread(FsDriftWorkload(self.params), 'fsdthr-1')
            t2 = TestThread(FsDriftWorkload(self.params), 'fsdthr-2')
            threads = [ t1, t2 ]
            for t in threads: 
                t.start()
            mylog = fsd_log.start_log('run2threads')
            mylog.info('threads started')
            time.sleep(2)
            touch(self.params.starting_gun_path)
            mylog.info('starting gun fired')
            for t in threads:
                t.join()
            mylog.info('threads done')
            for t in threads:
                print(t.worker.ctrs)
                t.worker.chk_status()
    unittest2.main()

