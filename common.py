# common.py - shared symbols and globals

import os, errno

# exception class so you know where exception came from

class FsDriftException(Exception):
    pass

NOTOK = 1
OK = 0
BYTES_PER_KB = 1 << 10
BYTES_PER_MB = 1 << 20
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
    HARDLINK = 9
    REMOUNT = 10


# file size can either be fixed or exponential random distribution

class FileSizeDistr:
    fixed = 0
    exponential = 1

def FileSizeDistr2str(v):
    if v == FileSizeDistr.fixed:
        return "fixed"
    elif v == FileSizeDistr.exponential:
        return "exponential"
    raise FsDriftException(
        'file size distribution must be one of: fixed, exponential')
 
# files are selected from population with random uniform 
# or gaussian distribution.

class FileAccessDistr:
    uniform = 2
    gaussian = 3

def FileAccessDistr2str(v):
    if v == FileAccessDistr.uniform:
        return "uniform"
    elif v == FileAccessDistr.gaussian:
        return "gaussian"
    raise FsDriftException(
        'file access distribution must be one of: uniform, gaussian')

# bit mask that allows selective enabling of debug messages
verbosity = 0
e = os.getenv("VERBOSITY")
if e != None:
    verbosity = int(e)
    print('verbosity = %u (0x%08x)' % (verbosity, verbosity))


# instead of looking up before deletion, do reverse, delete and catch exception

def ensure_deleted(file_path):
    try:
        os.unlink(file_path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise e


# just create an empty file
# leave exception handling to caller

def touch(fn):
    open(fn, 'w').close()


