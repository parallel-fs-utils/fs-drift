#!/usr/bin/python
# -*- coding: utf-8 -*-

'''
ssh_thread.py -- manages parallel execution of shell commands on remote hosts
Copyright 2012 -- Ben England
Licensed under the Apache License at http://www.apache.org/licenses/LICENSE-2.0
See Appendix on this page for instructions pertaining to license.
'''

import threading, os, errno, copy, time, subprocess, logging
# fs-drift modules
from fsd_log import start_log
from common import OK, NOTOK, FsDriftException

# this class is just used to create a python thread
# for each remote host that we want to use as a workload generator
# the thread just executes an ssh command to run this program on a remote host

class ssh_thread(threading.Thread):

    ssh_prefix = [ 'ssh', '-x', '-o', 'StrictHostKeyChecking=no' ]

    def __str__(self):
        return 'ssh-thread:%s:%s:%s' % \
            (self.remote_host, str(self.status), self.remote_cmd)

    def __init__(self, log, remote_host, remote_cmd):
        threading.Thread.__init__(self)
        self.remote_host = remote_host
        self.remote_cmd = remote_cmd
        self.status = NOTOK
        self.log = log
        self.args = copy.deepcopy(self.ssh_prefix)
        self.args.append(remote_host)
        self.args.append(remote_cmd)
        tmpdir = os.getenv('TMPDIR')
        if tmpdir == None:
            tmpdir = '/tmp'
        self.pidfile = os.path.join(tmpdir, self.remote_host + '_ssh.pid')
        self.popen_obj = None  # filled in latter

    def run(self):
        try:
            os.unlink(self.pidfile)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise e
        self.log.info(self.args)
        self.popen_obj = subprocess.Popen(self.args)
        with open(self.pidfile, 'w') as pid_f:
            pid_f.write('%d' % self.popen_obj.pid)
        self.popen_obj.wait()
        self.status = self.popen_obj.returncode

    def terminate(self):
        self.popen_obj.terminate()
        self.status = NOTOK

if __name__ == '__main__':
    import unittest2

    log = start_log('ssh_thread')

    class Test(unittest2.TestCase):
        def setUp(self):
            pass

        def test_a_mkThrd(self):
            sthrd = ssh_thread(log, 'localhost', 'sleep 1')
            sthrd.start()
            sthrd.join()
            if sthrd.status != OK:
                raise FsDriftException('return status %d' % sthrd.status)

        def test_b_abortThrd(self):
            sthrd = ssh_thread(log, 'localhost', 'sleep 60')
            sthrd.start()
            time.sleep(1)
            log.debug('subprocess pid is %d' % sthrd.popen_obj.pid)
            sthrd.terminate()
            assert(sthrd.status == NOTOK)

    unittest2.main()
