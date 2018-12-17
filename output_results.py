import time, sys
import os
import json

from fsop_counters import FSOPCounters
from common import FsDriftException, OK
from common import KiB_PER_GiB, BYTES_PER_KiB, MiB_PER_GiB, BYTES_PER_MiB


def print_stats(start_time, total_errors, fsop_ctrs):
    print('')
    print('elapsed time: %9.1f' % (time.time() - start_time))
    print('%9u = total errors' % total_errors)
    print(json.dumps(fsop_ctrs.json_dict(), indent=4))
    sys.stdout.flush()

def output_results(params, subprocess_list):
    cluster = FSOPCounters()
    rslt = {}
    rslt['hosts'] = {}
    print('host, thread, elapsed, files, I/O requests, MiB, status')
    fmt = '%s, %s, %f, %d, %d, %9.3f, %s'

    max_elapsed_time = 0.0
    for p in subprocess_list:
        max_elapsed_time = max(max_elapsed_time, p.elapsed_time)

    for p in subprocess_list:

        # add up work that it did
        # and determine time interval over which test ran

        status = 'ok'
        if p.status != OK:
            status = 'ERR: ' + str(p.status)

        c = p.ctrs
        c.add_to(cluster)

        thrd = {}
        thrd['status'] = status
        thrd['elapsed'] = p.elapsed_time
        thrd['files'] = c.total_files()
        thrd['ios'] = c.total_ios()
        thrd['MiB'] = c.total_bytes() / float(BYTES_PER_MiB)
        thrd['details'] = c.json_dict()
        if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly
            thrd['files-per-sec'] = thrd['files'] / max_elapsed_time
            thrd['IOPS'] = thrd['ios'] / max_elapsed_time
            thrd['MiB-per-sec'] = thrd['MiB'] / max_elapsed_time
        print(fmt %
              (p.onhost, p.tid, p.elapsed_time,
               thrd['files'], thrd['ios'], thrd['MiB'], status))

        # for JSON, show nesting of threads within hosts

        try:
            per_host_results = rslt['hosts'][p.onhost]
        except KeyError:
            per_host_results = { 'threads':{}, 'files':0, 'ios':0, 'MiB':0.0 }
            rslt['hosts'][p.onhost] = per_host_results
            per_host_counters = FSOPCounters()

        c.add_to(per_host_counters)
        per_host_results['details'] = per_host_counters.json_dict()
        per_host_results['threads'][p.tid] = thrd
        per_host_results['files'] = per_host_counters.total_files()
        per_host_results['ios'] = per_host_counters.total_ios()
        per_host_results['MiB'] = per_host_counters.total_bytes() / float(BYTES_PER_MiB)
        if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly
            per_host_results['host-files-per-sec'] = per_host_results['files'] / max_elapsed_time
            per_host_results['host-IOPS'] = per_host_results['files'] / max_elapsed_time
            per_host_results['host-MiB-per-sec'] = per_host_results['MiB'] / max_elapsed_time

    rslt['cluster-details'] = cluster.json_dict()

    print('total threads = %d' % len(subprocess_list))
    rslt['cluster-threads'] = len(subprocess_list)

    print('total files = %d' % cluster.total_files())
    rslt['cluster-files'] = cluster.total_files()

    print('total I/O requests = %d' % cluster.total_ios())
    rslt['cluster-ios'] = cluster.total_ios()

    total_MiB = cluster.total_bytes() / float(BYTES_PER_MiB)
    total_data_gb = total_MiB / MiB_PER_GiB
    print('total data = %9.3f GiB' % total_data_gb)
    rslt['cluster-data-GB'] = total_data_gb

    print('remounts = %d' % cluster.have_remounted)

    print('elapsed time = %9.3f' % max_elapsed_time)
    rslt['elapsed-time'] = max_elapsed_time

    if len(subprocess_list) < len(params.host_set) * params.threads:
        print('WARNING: failed to get some responses from workload generators')

    if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly

        files_per_sec = cluster.total_files() / max_elapsed_time
        print('files/sec = %f' % files_per_sec)
        rslt['cluster-files-per-sec'] = files_per_sec

        iops = float(cluster.total_ios()) / max_elapsed_time
        print('IOPS = %f' % iops)
        rslt['cluster-IOPS'] = iops

        mib_per_sec = total_MiB / max_elapsed_time
        print('MiB/sec = %f' % mib_per_sec)
        rslt['cluster-MiBps'] = mib_per_sec

    # if JSON output requested, generate it here

    if params.output_json_path:
        params_json_obj = params.to_json_obj()
        json_obj = {}
        json_obj['parameters'] = params_json_obj
        json_obj['results'] = rslt
        with open(params.output_json_path, 'w') as jsonf:
            json.dump(json_obj, jsonf, indent=4, sort_keys=True)

