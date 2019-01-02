# event.py - this module generates random I/O events in this simulation
# and also parses the workload specification which controls frequency of different event types

import sys
import random
# from fs-drift modules
import common
from common import rq, FsDriftException
from fsop import FSOPCtx
from fsop_counters import FSOPCounters

# read weights in from a CSV file where each record contains 
# operation name, relative weight
# operation name is one of names in fsop.FSOPCtx.opnames table
# relative weight is a non-negative floating-point number

def parse_weights(opts):
    linenum = 0
    weights = {}
    if opts.workload_table_csv_path != None:
        try:
            f = open(opts.workload_table_csv_path, 'r')
            lines = f.readlines()
            f.close()
            for l in lines:
                linenum += 1
                record = str.split(str.strip(l), ',')
                if len(record) < 2:
                    continue  # skip blank or partial lines
                (opname, relweight) = (record[0].strip(), record[1].strip())
                if opname.startswith('#') or opname == '':
                    continue
                try:
                    opcode = FSOPCtx.opname_to_opcode[opname]
                    weights[opcode] = float(relweight)
                    if weights[opcode] < 0.0:
                        raise FsDriftException('%s: negative weights not allowed' % 
                                                opts.workload_table_csv_path)
                except KeyError:
                    raise FsDriftException('%s: unrecognized opname' % opname)
                except ValueError:
                    raise FsDriftException('%s: relative frequency must be a floating-point number')
        except IOError as e:
            raise FsDriftException(
                'could not parse %s at line %d : %s' % (
                    opts.workload_table_csv_path, linenum, str(e)))
    else:
        raise FsDriftException('user must provide workload table')
    if len(weights) == 0:
        raise FsDriftException('workload table must not be empty')
    return weights


def print_weights(normalized_weights, out_f=sys.stdout):
    nl = '\n'
    out_f.write('normalized weights:' + nl)
    out_f.write('%20s  %9s   %9s' % (
        'request type', 'cum.prob', 'probability') + nl)
    last_weight = 0.0
    for (typ, cum_weight) in normalized_weights:
        name = FSOPCtx.opcode_to_opname[typ]
        weight = cum_weight - last_weight
        last_weight = cum_weight
        out_f.write('%20s     %5.3f    %5.3f' % (name, cum_weight, weight) + nl)
    out_f.write(nl)


# user-defined weights are then normalized to be
# probabilities here.  We sort them by weight 
# so that the loop will normally exit after very
# few iterations

def normalize_weights(weights):
    def extract_weight(weight_tuple):
        (typ,wgt) = weight_tuple
        return wgt
    total_weight = 0.0
    for (opcode, weight) in weights.items():
        total_weight += weight
    total_weight *= 1.01
    normalized_weights = []
    cum_probability = 0.0
    sorted_weights = sorted(weights.items(), reverse=True, key=extract_weight)
    for (typ, weight) in sorted_weights:
        probability = (float(weight)/total_weight)
        cum_probability += probability
        if cum_probability > 1.0 and cum_probability < 1.000001:
            # floating point noise, round it down to 1.0
            cum_probability = 1.0
        normalized_weights.append( (typ, cum_probability) )
    return normalized_weights

# FIXME: this could use binary search 
# or python equivalent if there are enough codes
# to justify avoiding linear search

def gen_event(normalized_weights):
    r = random.uniform(0.0, 1.0)
    for (opcode, cumulative_probability) in normalized_weights:
        last_opcode = opcode
        if r < cumulative_probability:
            return opcode
    return last_opcode

# unit test

if __name__ == '__main__':
    import opts
    import logging
    import fsd_log

    with open('/tmp/weights.csv', 'w') as w_f:
        w_f.write( '\n'.join( [
            'read, 2',
            'random_read, 2',
            'random_write, 2',
            'create, 6',
            'truncate, 0.2',
            'append, 4',
            'delete, 0.2',
            'hardlink, 0.3',
            'softlink, 0.3',
            'rename, 1',
            'remount,0.01',
            ]))
    params = opts.parseopts()
    params.workload_table_csv_path = '/tmp/weights.csv'
    weights = {}
    log = fsd_log.start_log('fsdevent')
    weights = parse_weights(params)
    normalized_weights = normalize_weights(weights)
    print_weights(normalized_weights)
    opcode_count = len(FSOPCtx.opname_to_opcode.keys())
    histogram = [0 for k in range(0, opcode_count)]

    # generate 10000 events and analyze frequency
    for i in range(0, 100000):
        opcode = gen_event(normalized_weights)
        histogram[opcode] += 1

    # print out histogram results
    for k in range(0, opcode_count):
        try:
            name = FSOPCtx.opcode_to_opname[k]
        except KeyError:
            continue
        count = histogram[k]
        print('%3d (%20s) : %6d' % (k, name, count))
