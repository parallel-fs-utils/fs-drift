#!/usr/bin/python
#
# parse_stress_log.py - parse a output file from fs-drift.py process run to obtain counter data
# output counter data in csv format with output field name in column 1, counters for each sample interval in right columns

import sys
import os

collect_counters = False
counters = {}
records = []
if len(sys.argv) > 1:
    with open(sys.argv[1], 'r') as f:
        records = [ r.strip() for r in f.readlines() ]
else:
    records = [ r.strip() for r in sys.stdin.readlines() ]

for r in records:
    if r.__contains__('elapsed time'):
        # we're made it past the test parameters, rest of output is counters
        collect_counters = True
        elapsed = (r.split()[2])
        try:
            counters['elapsed'].append(elapsed)
        except KeyError:
            counters['elapsed'] = [elapsed]
    if collect_counters and r.__contains__('='):  # if this is a counter record
        pair = [s.strip() for s in r.split('=')]
        key = pair[1]
        value = float(pair[0])
        try:
            counters[key].append(value)
        except KeyError as e:
            counters[key] = [value]
sample_ct = len(counters.values()[0])
for k in counters.keys():  # for each counter name
    for j in range(0, sample_ct):
        key_row = [k]
        key_row.extend( [ str(v) for v in counters[k] ] )
        print(','.join(key_row))
