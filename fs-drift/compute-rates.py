#!/usr/bin/python3
# compute rates from fs-drift counters saved in network shared dir
# aggregate them and save them 
#
# for now, to run it, you must run fs-drift with "--output-json pathname" option
# and then you must pass this pathname using environment variable
#   params_json_fn=pathname ./fs-drift-analyze.py network-shared-directory
# it gets the counter poll interval from there.
# hopefully we can get rid of this added complexity 
# once parameters are saved in JSON instead of pickle
# so then you could just run
#   ./fs-drift-analyze.py network-shared-directory
#
# it converts each value with a key containing the word "bytes" into MiB per sec,
# and substitutes "MBps" for "bytes" in the key name
# all other counters are in units of files per sec

from os import listdir, getenv
from os.path import join, isdir
from sys import argv, stdout, exit
from json import load, dump
from copy import deepcopy

BYTES_PER_KiB = 1024.0
BYTES_PER_MiB = BYTES_PER_KiB * BYTES_PER_KiB
BYTES_PER_GiB = BYTES_PER_MiB * BYTES_PER_KiB


def usage(errmsg):
    print('ERROR: %s' % errmsg)
    print('usage: fs-drift-analyze.py result-directory')
    exit(1)


def gen_rates_file_from_obj(rates_json_obj, result_directory, filename):
    rates_path = join(result_directory, '%s.json' % filename)
    with open(rates_path, 'w') as rates_f:
        dump(rates_json_obj, rates_f, indent=2)
        print('wrote %s' % rates_path)


# parse command line

if len(argv) < 2:
    usage('too few CLI parameters')
resultdir = argv[1]
if not isdir(resultdir):
    usage('%s is not directory' % resultdir)
file_list = [ l for l in listdir(resultdir) if l.startswith('counters') ]

# read in test params

params_json_fn = getenv('params_json_fn')
print('retrieving test parameters from file %s' % params_json_fn)
with open(params_json_fn) as params_f:
    prms = load(params_f)
stat_interval = float(prms['parameters']['stats report interval'])
print('counters interval = %f seconds' % stat_interval)

# extract result keys from any JSON

fn = join(file_list[0])
with open(join(resultdir, fn)) as f:
    jsonobj = load(f)
if len(jsonobj) < 1:
    usage('empty counters file %s' % fn)
example = jsonobj[0]
keys = example.keys()
fps_keys = [ k for k in keys if not k.__contains__('bytes') ]
bps_keys = [ k for k in keys if k.__contains__('bytes') ]

# now start computing rates from counters

thrd_rates = {}
for fn in file_list:
    fn_components = fn.split('.')
    thread_id = fn_components[1]
    # hostname may include DNS domain name
    host_id = '.'.join(fn_components[2:-1])
    global_thread_id = host_id + '.' + thread_id
    fn_path = join(resultdir, fn)
    with open(fn_path) as f:
        jsonobj = load(f)
    rate_list = []
    thrd_rates[global_thread_id] = rate_list
    for t, snapshot in enumerate(jsonobj):
        if t > 0:
            rate_dict = {}
            rate_list.append(rate_dict)
            for k in keys:
                v_t = float(snapshot[k])
                v_tm1 = float(jsonobj[t-1][k])
                delta = v_t - v_tm1
                rate = delta / stat_interval
                if k.__contains__('bytes'):
                    rate /= BYTES_PER_MiB
                    k = k.replace('bytes', 'MBps', 1)
                rate_dict[k] = rate

# add up per-thread rates to get host rates

host_rates = {}
for thrd in thrd_rates.keys():
    host = '.'.join(thrd.split('.')[:-1])
    host_key = 'host_' + host
    print('adding rates for thrd %s to host %s' % (thrd, host))
    try:
        h_rates = host_rates[host_key]
        for t, rate_snapshot in enumerate(h_rates):
            for rate_key in rate_snapshot.keys():
                rate_snapshot[rate_key] += float(thrd_rates[thrd][t][rate_key])
    except KeyError:
        host_rates[host_key] = deepcopy(thrd_rates[thrd])

# add up per-host rates to get cluster-wide rates
# assumption: all hosts are time-synced.

host_keys = host_rates.keys()
cluster_rate = None
for h in host_keys:
    print('adding host %s rates to cluster' % h)
    if cluster_rate is None:
        cluster_rate = deepcopy(host_rates[h])
    else:
        h_rates = host_rates[h]
        for t, rate_snapshot in enumerate(h_rates):
            for rate_key in rate_snapshot.keys():
                cluster_rate[t][rate_key] += float(rate_snapshot[rate_key])

gen_rates_file_from_obj(thrd_rates, resultdir, 'per-thread-rates')
gen_rates_file_from_obj(host_rates, resultdir, 'per-host-rates')
gen_rates_file_from_obj(cluster_rate, resultdir, 'cluster-rates')

