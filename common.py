# common.py - shared symbols and globals

import os, errno

# exception class so you know where exception came from

class FsDriftException(Exception):
    pass

NOTOK = 1
OK = 0
BYTES_PER_KiB = 1 << 10
BYTES_PER_MiB = 1 << 20
KiB_PER_GiB = 1 << 20
MiB_PER_GiB = 1 << 10
USEC_PER_SEC = 1000000
FD_UNDEFINED = -1


class rq:
    READ = 0
    RANDOM_READ = 1
    CREATE = 2
    RANDOM_WRITE = 3
    APPEND = 4
    SOFTLINK = 5
    HARDLINK = 6
    DELETE = 7
    RENAME = 8
    TRUNCATE = 9
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


# create directory if it's not already there

def ensure_dir_exists(dirpath):
    if not os.path.exists(dirpath):
        parent_path = os.path.dirname(dirpath)
        if parent_path == dirpath:
            raise FsDriftException(
                'ensure_dir_exists: cannot obtain parent path of non-existent path: ' +
                dirpath)
        ensure_dir_exists(parent_path)
        try:
            os.mkdir(dirpath)
        except OSError as e:
            if e.errno != errno.EEXIST:  # workaround for filesystem bug
                raise e
    else:
        if not os.path.isdir(dirpath):
            raise FsDriftException('%s already exists and is not a directory!'
                            % dirpath)

# careful with this one

def deltree(topdir):
    if len(topdir) < 6:
        raise FsDriftException('are you sure you want to delete %s ?' % topdir)
    if not os.path.exists(topdir):
        return
    if not os.path.isdir(topdir):
        return
    for (dir, subdirs, files) in os.walk(topdir, topdown=False):
        for f in files:
            os.unlink(os.path.join(dir, f))
        for d in subdirs:
            os.rmdir(os.path.join(dir, d))
    os.rmdir(topdir)

