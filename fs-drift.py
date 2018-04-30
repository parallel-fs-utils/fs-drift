#!/usr/bin/python2

# fs-drift.py - user runs this module to generate workload
# "-h" option generates online help

import os
import os.path
import time
import sys
import random
import event
import fsop
import common
from common import rq, OK, NOTOK, BYTES_PER_KB
import opts
import errno

# get byte counters from fsop

def refresh_counters():
        global counters
        counters = {'read' : fsop.read_bytes, 'create' : fsop.write_bytes, 'append' : fsop.write_bytes, 'random_write' : fsop.randwrite_bytes, 'random_read' : fsop.randread_bytes}
                
# instead of looking up before deletion, do reverse, delete and catch exception

def ensure_deleted(file_path):
	try:
		os.unlink(file_path)
	except OSError as e:
		if e.errno != errno.ENOENT:
			raise e

# print out counters for the interval that just completed.

def print_short_stats():
	print 'elapsed time: %9.1f'%(time.time() - start_time)
	print '\n'\
        '%9u = center\n' \
	'%9u = files created\t' \
        '%9u = files appended to\n' \
	'%9u = files random write\t' \
	'%9u = files read\n' \
	'%9u = files randomly read\n' \
	%(fsop.last_center, fsop.have_created, fsop.have_appended, fsop.have_randomly_written, \
	  fsop.have_read, fsop.have_randomly_read)
        sys.stdout.flush()

def print_stats():
	print
	print 'elapsed time: %9.1f'%(time.time() - start_time)
	print '\n\n'\
	'%9u = center\n' \
	'%9u = files created\n' \
	'%9u = files appended to\n' \
	'%9u = files randomly written to\n' \
	'%9u = files read\n' \
	'%9u = files randomly read\n' \
	'%9u = files truncated\n' \
	'%9u = files deleted\n' \
	'%9u = files renamed\n' \
	'%9u = softlinks created\n' \
        '%9u = hardlinks created\n' \
	%(fsop.last_center, fsop.have_created, fsop.have_appended, fsop.have_randomly_written, \
	  fsop.have_read, fsop.have_randomly_read, fsop.have_truncated, \
	  fsop.have_deleted, fsop.have_renamed, fsop.have_linked, fsop.have_hlinked)
	
	print \
	'%9u = read requests\n' \
	'%9u = read bytes\n'\
	'%9u = random read requests\n' \
	'%9u = random read bytes\n' \
	'%9u = write requests\n' \
	'%9u = write bytes\n'\
	'%9u = random write requests\n' \
	'%9u = random write bytes\n' \
	'%9u = fdatasync calls\n' \
	'%9u = fsync calls\n' \
	'%9u = leaf directories created\n' \
	%(fsop.read_requests, fsop.read_bytes, fsop.randread_requests, fsop.randread_bytes, \
	  fsop.write_requests, fsop.write_bytes, fsop.randwrite_requests, fsop.randwrite_bytes, \
	  fsop.fdatasyncs, fsop.fsyncs, fsop.dirs_created)
	
	print \
	'%9u = no create -- file already existed\n'\
	'%9u = file not found\n'\
	%(fsop.e_already_exists, fsop.e_file_not_found)
	print \
	'%9u = no directory space\n'\
	'%9u = no space for new inode\n'\
	'%9u = no space for write data\n'\
	%(fsop.e_no_dir_space, fsop.e_no_inode_space, fsop.e_no_space)
	print '%9u = total errors'%total_errors
        sys.stdout.flush()

# the main program

opts.parseopts()
event.parse_weights()
event.normalize_weights()
total_errors = 0
fsop.init_buf()

try:
	os.mkdir(opts.top_directory)
except os.error, e:
	if e.errno != errno.EEXIST:
		raise e
if opts.rsptimes:
	rsptime_filename = '/var/tmp/fs-drift_rsptimes_%d_%d_rspt.csv'%(int(time.time()) , os.getpid())
	rsptime_file = open(rsptime_filename, "w")

if opts.bw:
	bw_filename = '/var/tmp/fs-drift_bw_%d_%d_bw.csv'%(int(time.time()) , os.getpid())
	bw_file = open(bw_filename, "w")

os.chdir(opts.top_directory)
sys.stdout.flush()

op = 0
bandwidth = {'read':'read', 'random_read': 'random_read', 'create':'write', 'random_write':'random_write', 'append':'write'}

last_stat_time = time.time()
last_drift_time = time.time()
stop_file = opts.top_directory + os.sep + 'stop-file'

# we have to synchronize threads across multiple hosts somehow, we do this with a 
# file in a shared file system.

if opts.starting_gun_file:
  while not os.access(opts.starting_gun_file, os.R_OK):
    time.sleep(1)
time.sleep(2) # give everyone else a chance to see that start-file is there
start_time = time.time()
event_count = 0

while True:
        #if there is pause file present, do nothing

        if os.path.isfile(opts.pause_file):
                time.sleep(5)
                continue

        # every 1000 events, check for "stop file" that indicates test should end

	event_count += 1
        if (event_count % 1000 == 0) and os.access(stop_file, os.R_OK):
		break

	# if using operation count to limit test

	if opts.opcount > 0:
		if op >= opts.opcount: break
		op += 1

	# if using duration to limit test

	if opts.duration > 0:
		elapsed = time.time() - start_time
		if elapsed > opts.duration: 
			break
	x = event.gen_event()
	(fn, name) = fsop.rq_map[x]
	if common.verbosity & 0x1: 
		print
		print x, name
        before_drift = time.time()
        curr_e_exists, curr_e_not_found = fsop.e_already_exists, fsop.e_file_not_found
        refresh_counters()
        if name in counters:
                bytes_before = counters[name] 
	try:
		rc = fn()
		after = fsop.time_after
		before = fsop.time_before
		if curr_e_exists == fsop.e_already_exists and curr_e_not_found == fsop.e_file_not_found:
		        total_time = float(after - before)
		        if opts.rsptimes:
		                rsptime_file.write('%9.3f , %9.6f , %s\n'%(before - start_time,  total_time, name))
		        if name in counters and opts.bw:
		                refresh_counters()
		                total_size = counters[name] - bytes_before
		                bw_file.write('%9.3f , %9.6f , %s\n'%(before - start_time,  (total_size / total_time)/BYTES_PER_KB, bandwidth[name]))
	except KeyboardInterrupt, e:
		print "received SIGINT (control-C) signal, aborting..."
		break
	if rc != OK:
		print "%s returns %d"%(name, rc)
		total_errors += 1
	if (opts.stats_report_interval > 0) and (before - last_stat_time > opts.stats_report_interval):
                if opts.short_stats == True:
                        print_short_stats()
                else:
		        print_stats()
		last_stat_time = before
	if (opts.drift_time > 0) and (before_drift - last_drift_time > opts.drift_time):
                fsop.simulated_time += opts.drift_time
		last_drift_time = before_drift

if opts.rsptimes:
	rsptime_file.close()
	print 'response time file is %s'%rsptime_filename
	
if opts.bw:
	bw_file.close()
	print 'bandwidth file is %s'%bw_filename

print_stats()
if opts.starting_gun_file: ensure_deleted(opts.starting_gun_file)
ensure_deleted(stop_file)
