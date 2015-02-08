# event.py - this module generates random I/O events in this simulation
# and also parses the workload specification which controls frequency of different event types

import string
import sys
import random
# from fsstress modules
import common
from common import rq
import fsop
import opts

# normalize weights and compute table that maps random # into rq type
# FIXME: read weights in from user

#default weights are:

weights = { rq.READ:10, rq.RANDOM_READ:2, rq.CREATE:4, rq.RANDOM_WRITE:2, rq.APPEND:2, rq.LINK:1, rq.DELETE:1, rq.RENAME:1, rq.TRUNCATE:1 }

normalized_weights = {}

def parse_weights():
  global weights
  linenum = 1
  if opts.workload_table_filename != None:
    try:
	f = open(opts.workload_table_filename, 'r')
	lines = f.readlines()
	f.close()
	weights = {}
	for l in lines:
		record = string.split(string.strip(l),',')
		if len(record) < 2: continue # skip blank or partial lines
		(opname, relweight) = (record[0], record[1])
		for (opcode, (opfn, right_opname)) in fsop.rq_map.items():
			if right_opname == string.lower(opname):
				weights[opcode] = int(relweight)
		linenum += 1
    except IOError, e:
	print str(e)
	print 'could not parse workload.csv at line %d'%linenum
	sys.exit(1)
  print 'weights: %s'%str(weights)

def normalize_weights():
	global normalized_weights
	total_weight = 0.0
	for (opcode, weight) in weights.items(): 
		total_weight += weight
	normalized_weights = {}
	print 'total weight: %f '%total_weight
	print 'normalized weights:'
	print '%20s  %9s   %6s  %9s'%('request type', 'weight', 'cum.prob', 'probability')
	cum_probability = 0.0
	for (typ, weight) in weights.items():
		probability = (float(weight)/total_weight)
		cum_probability += probability
		normalized_weights[typ] = cum_probability
		(fn, name) = fsop.rq_map[typ]
		print '%20s  %9u   %5.3f      %5.3f'%\
			(name, weight, cum_probability, probability)
	print


def gen_event():
	r = random.uniform(0.0,1.0)
	if common.verbosity & 0x200000: print 'random event generator = %f'%r
	for (opcode, cumulative_probability) in normalized_weights.items():
		(fn, opname) = fsop.rq_map[opcode]
		if common.verbosity & 0x100: print opcode, opname, cumulative_probability
		if r < cumulative_probability:
			return opcode;
	return opcode

if __name__ == '__main__':
	histogram = range(0, len(weights))
	for i in range(0,1000):
		histogram[gen_event()] += 1
	print histogram
