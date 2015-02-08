# common.py - symbols used throughout fsstress

import os

NOTOK = 1
OK = 0
BYTES_PER_KB = 1<<10
BYTES_PER_MB = 1<<20
FD_UNDEFINED = -1

class rq:
	READ = 0
	RANDOM_READ = 1
	CREATE = 2
	RANDOM_WRITE = 3
	APPEND = 4
	LINK = 5
	DELETE = 6
	RENAME = 7
	TRUNCATE = 8

class file_access_dist:
	UNIFORM = 2
	GAUSSIAN = 3

# bit mask that allows selective enabling of debug messages
verbosity = 0
e = os.getenv("VERBOSITY")
if e != None:
	verbosity = int(e)
	print 'verbosity = %u (0x%08x)'%(verbosity,verbosity)

