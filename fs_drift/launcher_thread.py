# -*- coding: utf-8 -*-

'''
launcher_thread.py
manages parallel execution of shell commands on remote hosts
it assumes there is a poller on each remote host, launch_smf_host.py,
it waits for files of form '*.smf_launch' in the shared directory
and when it finds one,
it reads in the command to start the worker from it and launches it.
This takes the place of an sshd thread launching it.
Copyright 2012 -- Ben England
Licensed under the Apache License at http://www.apache.org/licenses/LICENSE-2.0
See Appendix on this page for instructions pertaining to license.
'''

import threading
import os
import time
from common import ensure_deleted, OK, NOTOK
from os.path import join
import sync_files

# this class is just used to create a python thread
# for each remote host that we want to use as a workload generator
# the thread just executes an ssh command to run this program on a remote host

class launcher_thread(threading.Thread):

    def __init__(self, params, log, remote_host, remote_cmd_in):
        threading.Thread.__init__(self)
        self.params = params
        self.log = log
        self.remote_host = remote_host
        self.launch_fn = join(
                            self.params.network_shared_path,
                            self.remote_host + '.fsd_launch')
        self.pickle_fn = join(
                            self.params.network_shared_path, 
                            self.remote_host + '_result.pickle')
        self.remote_cmd = remote_cmd_in
        self.status = None

    def run(self):
        ensure_deleted(self.launch_fn)
        ensure_deleted(self.pickle_fn)
        with open(self.launch_fn, 'w') as launch_file:
            launch_file.write(self.remote_cmd)
            launch_file.close()
        self.log.debug('waiting for pickle file %s' % self.pickle_fn)
        self.status = NOTOK  # premature exit means failure
        while not os.path.exists(self.pickle_fn):
            if os.path.exists(self.params.abort_path):
                self.log.info('test abort seen by host ' + self.remote_host)
                return
            time.sleep(2)
        self.status = OK  # success!

    def terminate(self):
        sync_files.write_sync_file(self.params.abort_path, 'shut it down')
        self.status = NOTOK

