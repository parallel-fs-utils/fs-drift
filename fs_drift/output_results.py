import time
import sys
import os
import json
import copy
from fs_drift.fsop_counters import FSOPCounters
from fs_drift.common import FsDriftException, OK
from fs_drift.common import KiB_PER_GiB, BYTES_PER_KiB, MiB_PER_GiB, BYTES_PER_MiB


def output_thread_counters(outfile, start_time, total_errors, fsop_ctrs):
    jsondict = fsop_ctrs.json_dict()
    jsondict['elapsed-time'] = '%9.1f' % (time.time() - start_time)
    jsondict['total-errors'] = '%9u' % total_errors
    outfile.write(json.dumps(jsondict, indent=4) + '\n')
    outfile.flush()


def output_results(params, subprocess_list):
    cluster = FSOPCounters()
    host_index = 0
    host_ids = {}
    rslt = {}
    rslt['in-host'] = {}
    if not params.rawdevice:
        print('host, thread, elapsed, files, I/O requests, MiB, status')
        fmt = '%s, %s, %f, %d, %d, %9.3f, %s'
    else:
        print('host, thread, elapsed, I/O requests, MiB, status')
        fmt = '%s, %s, %f, %d, %9.3f, %s'

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
        thrd['fsop-counters'] = c.json_dict()
        if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly
            thrd['files-per-sec'] = thrd['files'] / max_elapsed_time
            thrd['IOPS'] = thrd['ios'] / max_elapsed_time
            thrd['MiB-per-sec'] = thrd['MiB'] / max_elapsed_time
        if not params.rawdevice:
            print(fmt % (p.onhost, p.tid, p.elapsed_time, thrd['files'], thrd['ios'], thrd['MiB'], status))
        else:
            print(fmt % (p.onhost, p.tid, p.elapsed_time, thrd['ios'], thrd['MiB'], status))

        # for JSON, show nesting of threads within hosts
        # first map host name to host number in test

        try:
            host_number = host_ids[p.onhost]
        except KeyError:
            host_ids[p.onhost] = host_index
            host_number = host_index
            host_index += 1

        try:
            per_host_results = rslt['in-host'][host_number]
        except KeyError:
            per_host_results = {'hostname': p.onhost, 'in-thread': {}, 'files': 0, 'ios': 0, 'MiB': 0.0}
            rslt['in-host'][host_number] = per_host_results
            per_host_counters = FSOPCounters()

        c.add_to(per_host_counters)
        per_host_results['fsop-counters'] = per_host_counters.json_dict()
        per_host_results['in-thread'][p.tid] = thrd
        per_host_results['files'] = per_host_counters.total_files()
        per_host_results['ios'] = per_host_counters.total_ios()
        per_host_results['MiB'] = per_host_counters.total_bytes() / float(BYTES_PER_MiB)
        if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly
            per_host_results['files-per-sec'] = per_host_results['files'] / max_elapsed_time
            per_host_results['IOPS'] = per_host_results['ios'] / max_elapsed_time
            per_host_results['MiB-per-sec'] = per_host_results['MiB'] / max_elapsed_time

    # if only 1 host, remove a redundant level in the hierarchy
    # since host results = cluster results

    if len(rslt['in-host']) == 1 and params.host_set == []:
        rslt['in-thread'] = {}
        thread_set = copy.deepcopy(rslt['in-host'][0]['in-thread'])
        for tid in thread_set.keys():
            rslt['in-thread'][tid] = copy.deepcopy(thread_set[tid])
        del rslt['in-host']

    rslt['fsop-counters'] = cluster.json_dict()

    print('total threads = %d' % len(subprocess_list))
    rslt['threads'] = len(subprocess_list)

    if not params.rawdevice:
        print('total files = %d' % cluster.total_files())
        rslt['files'] = cluster.total_files()

    print('total I/O requests = %d' % cluster.total_ios())
    rslt['ios'] = cluster.total_ios()

    total_MiB = cluster.total_bytes() / float(BYTES_PER_MiB)
    total_data_gb = total_MiB / MiB_PER_GiB
    print('total data = %9.3f GiB' % total_data_gb)
    rslt['MiB'] = total_data_gb

    if cluster.have_remounted > 0:
        print('remounts = %d' % cluster.have_remounted)

    print('elapsed time = %9.3f' % max_elapsed_time)
    rslt['elapsed'] = max_elapsed_time

    if len(subprocess_list) < len(params.host_set) * params.threads:
        print('WARNING: failed to get some responses from workload generators')

    now = time.time()
    start_time = now - max_elapsed_time
    rslt['start-time'] = start_time
    rslt['date'] = time.strftime('%Y-%m-%dT%H:%M:%S.000Z', time.gmtime(start_time))

    if max_elapsed_time > 0.001:  # can't compute rates if it ended too quickly

        if not params.rawdevice:
            files_per_sec = cluster.total_files() / max_elapsed_time
            print('files/sec = %f' % files_per_sec)
            rslt['files-per-sec'] = files_per_sec

        iops = float(cluster.total_ios()) / max_elapsed_time
        print('IOPS = %f' % iops)
        rslt['IOPS'] = iops

        mib_per_sec = total_MiB / max_elapsed_time
        print('MiB/sec = %f' % mib_per_sec)
        rslt['MiB-per-sec'] = mib_per_sec

    # if JSON output requested, generate it here

    if params.output_json_path:
        params_json_obj = params.to_json_obj()
        json_obj = {}
        json_obj['parameters'] = params_json_obj
        json_obj['results'] = rslt
        with open(params.output_json_path, 'w') as jsonf:
            json.dump(json_obj, jsonf, indent=4, sort_keys=True)
