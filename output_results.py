import time, sys
import os
import json

import fsop
from common import FsDriftException, OK
from common import KiB_PER_GiB, BYTES_PER_KiB, MiB_PER_GiB, BYTES_PER_MiB


def print_stats(start_time, total_errors, fsop_ctrs):
    print('')
    print('elapsed time: %9.1f' % (time.time() - start_time))
    print('%9u = total errors' % total_errors)
    print(json.dumps(fsop_ctrs.json_dict(), indent=4))
    sys.stdout.flush()

def output_results(params, subprocess_list):
    total_files = 0
    total_ios = 0
    total_MiB = 0.0
    max_elapsed_time = 0.0
    rslt = {}
    rslt['hosts'] = {}
    print('host, thread, elapsed, files, I/O requests, MiB, status')
    fmt = '%s, %s, %f, %d, %d, %9.3f, %s'
    for p in subprocess_list:
        max_elapsed_time = max(max_elapsed_time, p.elapsed_time)
    for p in subprocess_list:

        # add up work that it did
        # and determine time interval over which test ran

        status = 'ok'
        if p.status != OK:
            status = 'ERR: ' + str(p.status)

        c = p.ctrs
        total_thread_files = c.have_created + c.have_deleted + c.have_softlinked + \
                        c.have_hardlinked + c.have_appended + c.have_randomly_written + \
                        c.have_read + c.have_randomly_read + c.have_truncated

        total_thread_ios = c.read_requests + c.randread_requests + c.write_requests + c.randwrite_requests

        total_thread_bytes = c.read_bytes + c.read_bytes + c.randwrite_bytes + c.write_bytes
        total_thread_MiB = float(total_thread_bytes) / BYTES_PER_MiB

        print(fmt %
              (p.onhost, p.tid, p.elapsed_time,
               total_thread_files, total_thread_ios, total_thread_MiB, status))

        per_thread_obj = {}
        per_thread_obj['status'] = status
        per_thread_obj['elapsed'] = p.elapsed_time
        per_thread_obj['thr-files'] = total_thread_files
        per_thread_obj['thr-ios'] = total_thread_ios
        per_thread_obj['thr-MiB'] = total_thread_MiB
        if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly
            per_thread_obj['thr-files-per-sec'] = per_thread_obj['thr-files'] / max_elapsed_time
            per_thread_obj['thr-IOPS'] = per_thread_obj['thr-ios'] / max_elapsed_time
            per_thread_obj['thr-MiB-per-sec'] = per_thread_obj['thr-MiB'] / max_elapsed_time

        # for JSON, show nesting of threads within hosts

        try:
            per_host_results = rslt['hosts'][p.onhost]
        except KeyError:
            per_host_results = { 'threads':{}, 'files':0, 'ios':0, 'MiB':0.0 }
            rslt['hosts'][p.onhost] = per_host_results
        per_host_results['threads'][p.tid] = per_thread_obj
        per_host_results['files'] += total_thread_files
        per_host_results['ios'] += total_thread_ios
        per_host_results['MiB'] += total_thread_MiB
        if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly
            per_host_results['host-files-per-sec'] = per_host_results['files'] / max_elapsed_time
            per_host_results['host-IOPS'] = per_host_results['files'] / max_elapsed_time
            per_host_results['host-MiB-per-sec'] = per_host_results['MiB'] / max_elapsed_time
        # aggregate to get stats for whole run

        total_files += total_thread_files
        total_ios += total_thread_ios
        total_MiB += total_thread_MiB

    print('total threads = %d' % len(subprocess_list))
    rslt['cluster-threads'] = len(subprocess_list)

    print('total files = %d' % total_files)
    rslt['cluster-files'] = total_files

    print('total I/O requests = %d' % total_ios)
    rslt['cluster-ios'] = total_ios

    total_data_gb = total_MiB / MiB_PER_GiB
    print('total data = %9.3f GiB' % total_data_gb)
    rslt['cluster-data-GB'] = total_data_gb

    print('elapsed time = %9.3f' % max_elapsed_time)
    rslt['elapsed-time'] = max_elapsed_time

    if len(subprocess_list) < len(params.host_set) * params.threads:
        print('WARNING: failed to get some responses from workload generators')

    if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly

        files_per_sec = total_files / max_elapsed_time
        print('files/sec = %f' % files_per_sec)
        rslt['cluster-files-per-sec'] = files_per_sec

        iops = float(total_ios) / max_elapsed_time
        print('IOPS = %f' % iops)
        rslt['cluster-IOPS'] = iops

        mb_per_sec = total_MiB / max_elapsed_time
        print('MiB/sec = %f' % mb_per_sec)
        rslt['cluster-MiBps'] = mb_per_sec

    # if JSON output requested, generate it here

    if params.output_json_path:
        params_json_obj = params.to_json_obj()
        json_obj = {}
        json_obj['parameters'] = params_json_obj
        json_obj['results'] = rslt
        with open(params.output_json_path, 'w') as jsonf:
            json.dump(json_obj, jsonf, indent=4, sort_keys=True)

