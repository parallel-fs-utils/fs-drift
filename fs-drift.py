#!/usr/bin/python2

# fs-drift.py - user runs this module to generate workload
# "-h" option generates online help

import os
import os.path
import time
import sys
import random
import event
import errno
import pickle

import fsop
import common
from common import rq, OK, NOTOK
import opts
import output_results

# the main program


params = opts.parseopts()
event.parse_weights(params)
event.normalize_weights()
total_errors = 0
fsop.init_buf(params)
if len(params.top_directory) < 6:
    raise FsDriftException(
            'top directory %s too short, may be system directory' % 
            params.top_directory)

try:
    os.mkdir(params.network_shared_path)
except os.error as e:
    if e.errno != errno.EEXIST:
        raise e

# save params in a place where remote workload generators can read them

with open(params.param_pickle_path, 'w') as pickle_f:
    pickle.dump(params, pickle_f)
    
os.chdir(params.top_directory)
sys.stdout.flush()

op = 0
rsptimes = {'read': [], 'random_read': [], 'create': [], 'random_write': [], 'append': [
], 'link': [], 'delete': [], 'rename': [], 'truncate': [], 'hardlink': []}
last_stat_time = time.time()
last_drift_time = time.time()
stop_file = params.stop_file_path

# we have to synchronize threads across multiple hosts somehow, we do this with a
# file in a shared file system.

#if params.starting_gun_file:
#    while not os.access(params.starting_gun_file, os.R_OK):
#        time.sleep(1)
#time.sleep(2)  # give everyone else a chance to see that start-file is there
start_time = time.time()
event_count = 0

while True:
    # if there is pause file present, do nothing

    if os.path.isfile(params.pause_file):
        time.sleep(5)
        continue

    # every 1000 events, check for "stop file" that indicates test should end

    event_count += 1
    if (event_count % 1000 == 0) and os.access(stop_file, os.R_OK):
        break

    # if using operation count to limit test

    if params.opcount > 0:
        if op >= params.opcount:
            break
        op += 1

    # if using duration to limit test

    if params.duration > 0:
        elapsed = time.time() - start_time
        if elapsed > params.duration:
            break
    x = event.gen_event()
    (fn, name) = fsop.rq_map[x]
    if common.verbosity & 0x1:
        print()
        print(x, name)
    before = time.time()
    before_drift = time.time()
    curr_e_exists, curr_e_not_found = fsop.e_already_exists, fsop.e_file_not_found
    try:
        rc = fn(params)
        after = time.time()
        if curr_e_exists == fsop.e_already_exists and curr_e_not_found == fsop.e_file_not_found:
            rsptimes[name].append((before - start_time, after - before))
    except KeyboardInterrupt as e:
        print("received SIGINT (control-C) signal, aborting...")
        break
    if rc != OK:
        print("%s returns %d" % (name, rc))
        total_errors += 1
    if (params.stats_report_interval > 0) and (before - last_stat_time > params.stats_report_interval):
        if params.short_stats == True:
            output_results.print_short_stats(start_time)
        else:
            output_results.print_stats(start_time, total_errors)
        last_stat_time = before
    if (params.drift_time > 0) and (before_drift - last_drift_time > params.drift_time):
        fsop.simulated_time += params.drift_time
        last_drift_time = before_drift

if params.rsptimes:
    for key, ls in list(rsptimes.items()):
        rsptime_file.write(key+'\n')
        for (reltime, rspt) in ls:
            rsptime_file.write('%9.3f , %9.6f\n' % (reltime,  rspt))
    rsptime_file.close()
    print('response time file is %s' % rsptime_filename)

output_results.print_stats(start_time, total_errors)
if params.starting_gun_file:
    common.ensure_deleted(params.starting_gun_file)
common.ensure_deleted(stop_file)
