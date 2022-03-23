#!/usr/bin/python3
# this script graphs a JSON file containing fs-drift counter rates
# input parameters:
#   1: .json file produced by fs-drift and compute_rates.py
#   2: counter type : fps (for files-per-second) or MBps (for MiB/sec)
#   3: time interval covered by each set of rates (sec)
#   4: [optional] entity to graph
# some .json files contain data for multiple threads or multiple hosts
# in these cases you have to choose one, and it will help you by listing possibilities
# so for example:
#   $ graph-json-rates.py /mnt/cephfs/network-shared/cluster-rates.json MBps 5
#   $ graph-json-rates.py /mnt/cephfs/network-shared/per-host-rates.json fps 5 localhost.localdomain.00 
# it generates a .png file containing the desired graph with a unique filename 
# in the same directory as the .json file that generated it
# if you don't want to see the graph and you just want .png file, unset DISPLAY env. var.

import sys
from sys import argv
import os.path
import matplotlib.pyplot as plt
from matplotlib.transforms import Bbox
import json

def usage(errmsg):
    print('ERROR: %s' % errmsg)
    print('usage: graph-json-rates.py fs-drift-rates-json-file fps|mbps interval-sec [ optional-key ] ')
    sys.exit(1)

if len(argv) < 3:
    usage('not enough CLI parameters')
input_json_path = argv[1]
want_fps = (argv[2] == 'fps')
try:
    interval_sec = float(argv[3])
except ValueError:
    usage('could not parse time interval %s' % str(argv[2]))

select_key = None
if len(argv) == 5:
    select_key = argv[4]
with open(input_json_path) as f:
  data = json.load(f)
print('JSON result file: %s' % input_json_path)
print('want to graph %s' % 'files/sec' if want_fps else 'MiB/sec')
print('thread/host key: %s' % select_key if select_key is not None else 'cluster-wide results')

vars = {}
if isinstance(data, list):
    if select_key is not None:
        usage('result file does not have outer dictionary, do not use key')
    # this is probably the entire cluster
    selected_entity_rates = data
else:
    entities = list(data.keys())
    print('list of entities in this file is: %s' % str(entities))
    if select_key is None:
        usage('you must specify key for one of the entities in this file')
    selected_entity_rates = data[select_key]

key_source = selected_entity_rates[0].keys()
for k in key_source:
    vars[k] = []
rate_intervals = len(selected_entity_rates)
elapsed_time = rate_intervals * interval_sec
print('elapsed time of test run: %f' % interval_sec)

# separate out the files-per-sec and bytes-per-sec values

for d in selected_entity_rates:
    for k in d.keys():
        vars[k].append(d[k])
files_data = {}
bytes_data = {}
max_value = 0
for k in d.keys():
    if k.__contains__('MBps') and not want_fps:
        bytes_data[k] = vars[k]
        max_value = max(max_value, max(vars[k]))
        sample_count = len(vars[k])
    elif k.__contains__('elapsed') or k.__contains__('total-errors'):
        pass
    elif (not k.__contains__('MBps')) and want_fps:
        files_data[k] = vars[k]
        max_value = max(max_value, max(vars[k]))
        sample_count = len(vars[k])


# call pyplot to generate the graph

bytes_lines = []
files_lines = []
plot_number = 111

font = {'family':   'serif',
        'color':    'blue',
        'weight':   'bold',
        'size':     16,
        }

fig, ax = plt.subplots()
plt.subplot(plot_number)
if want_fps:
    for k in files_data.keys():
        files_lines.append(ax.plot(range(0,sample_count), files_data[k], label=k))
else:
    for k in bytes_data.keys():
        bytes_lines.append(ax.plot(range(0,sample_count), bytes_data[k], label=k))

ax.legend(loc='upper right', fontsize='medium',bbox_to_anchor=(1.1,1.0))
ax.set_ylim([0.0, max_value])
plt.xlabel('fraction of elapsed time', fontdict=font)
plt.ylabel('files/sec' if want_fps else 'MiB/sec', fontdict=font)
title = 'fs-drift rates'
if select_key is not None:
    title += ' for %s' % select_key
else:
    title += ' for entire cluster'
plt.title(title, fontdict=font)
plt.grid(True)
plt.show()

# save .png file containing graph for inclusion in reports, etc.

input_directory = os.path.dirname(input_json_path)
input_filename = os.path.basename(input_json_path).split('.')[0]
if select_key is not None:
    input_filename += '_for_%s' % select_key
output_path = os.path.join(input_directory, input_filename+'.png')
print('writing graph to %s' % output_path)
fig.savefig(output_path)


