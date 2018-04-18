#!/usr/bin/python
#
# parse_stress_log.py - parse a output file from fs-drift.py process run to obtain counter data
# output counter data in csv format with output field name in column 1, counters for each sample interval in right columns

import sys
import os
import list2csv

collect_counters = False
counters = {}
records = [r.strip() for r in sys.stdin.readlines()]
for r in records:
    if r.__contains__('elapsed time') and r.__contains__(' 0.0'):
        # we're made it past the test parameters, rest of output is counters
        collect_counters = True
    if collect_counters and r.__contains__('='):  # if this is a counter record
        pair = [s.strip() for s in r.split('=')]
        key = pair[1]
        value = float(pair[0])
        try:
            counters[key].append(value)
        except KeyError as e:
            counters[key] = [value]
for k in list(counters.keys()):  # for each counter name
    # output list of samples for this thread
    print(k + ',' + list2csv.list2csv(counters[k]))
