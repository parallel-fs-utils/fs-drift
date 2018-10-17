# fsop.py - module containing filesystem operation types and common code for them

# NOTE: this version requires "numpy" rpm to be installed
# std python modules
import os
import os.path
import random
import errno
import time
import random_buffer
# my modules
import common
from common import rq, file_access_dist, verbosity, OK, NOTOK, BYTES_PER_KB, FD_UNDEFINED
import opts
import numpy  # for gaussian distribution
import subprocess

# operation counters, incremented by op function below
have_created = 0
have_deleted = 0
have_linked = 0
have_written = 0
have_appended = 0
have_randomly_written = 0
have_read = 0
have_randomly_read = 0
have_renamed = 0
have_truncated = 0
have_hlinked = 0

# throughput counters
read_requests = 0
read_bytes = 0
randread_requests = 0
randread_bytes = 0
write_requests = 0
write_bytes = 0
randwrite_requests = 0
randwrite_bytes = 0
fsyncs = 0
fdatasyncs = 0
dirs_created = 0

# time counters
time_before = 0
time_after = 0

# error counters
e_already_exists = 0
e_file_not_found = 0
e_no_dir_space = 0
e_no_inode_space = 0
e_no_space = 0

# most recent center
last_center = 0


# someday these two should be parameters
total_dirs = 1

link_suffix = '.s'
hlink_suffix = '.h'
rename_suffix = '.r'

buf = None

large_prime = 12373

# for gaussian distribution with moving mean, we need to remember simulated time
# so we can pick up where we left off with moving mean

simtime_pathname = '/var/tmp/fs-drift-simtime.tmp'
SIMULATED_TIME_UNDEFINED = None
simulated_time = SIMULATED_TIME_UNDEFINED  # initialized later
time_save_rate = 5


def init_buf():
    global buf
    buf = random_buffer.gen_buffer(opts.max_record_size_kb*BYTES_PER_KB)

def refresh_buf(size):
    global buf
    buf = random_buffer.gen_buffer(size)

def scallerr(msg, fn, syscall_exception):
    err = syscall_exception.errno
    a = subprocess.Popen("date", shell=True,
                         stdout=subprocess.PIPE).stdout.read()
    print('%s ERROR: %s: %s syscall errno %d(%s)' % (
        a.rstrip('\n'), msg, fn, err, os.strerror(err)))


def gen_random_dirname(file_index):
    d = '.'
    # multiply file_index ( < opts.max_files) by large number relatively prime to dirs_per_level
    index = file_index * large_prime
    for j in range(0, opts.levels):
        subdir_index = 1 + (index % opts.dirs_per_level)
        dname = 'd%04d' % subdir_index
        d = os.path.join(d, dname)
        index /= opts.dirs_per_level
    return d


def gen_random_fn(is_create=False):
    global total_dirs
    global simulated_time
    global last_center
    if total_dirs == 1:  # if first time
        for i in range(0, opts.levels):
            total_dirs *= opts.dirs_per_level
    max_files_per_dir = opts.max_files // total_dirs

    if opts.rand_distr_type == file_access_dist.UNIFORM:
        # lower limit 0 means at least 1 file/dir
        index = random.randint(0, max_files_per_dir)
    elif opts.rand_distr_type == file_access_dist.GAUSSIAN:

        # if simulated time is not defined,
        # attempt to read it in from a file, set to zero if no file

        if simulated_time == SIMULATED_TIME_UNDEFINED:
            try:
                with open(simtime_pathname, 'r') as readtime_fd:
                    simulated_time = int(readtime_fd.readline().strip())
            except IOError as e:
                if e.errno != errno.ENOENT:
                    raise e
                simulated_time = 0
            print(('resuming with simulated time %d' % simulated_time))

        # for creates, use greater time, so that reads, etc. will "follow" creates most of the time
        # mean and std deviation define gaussian distribution

        center = (simulated_time * opts.mean_index_velocity)
        if is_create:
            center += (opts.create_stddevs_ahead * opts.gaussian_stddev)
        if verbosity & 0x20:
            print('%f = center' % center)
        index_float = numpy.random.normal(
            loc=center, scale=opts.gaussian_stddev)
        file_opstr = 'read'
        if is_create:
            file_opstr = 'create'
        if verbosity & 0x20:
            print('%s gaussian value is %f' % (file_opstr, index_float))
        #index = int(index_float) % max_files_per_dir
        index = int(index_float) % opts.max_files
        last_center = center

        # since this is a time-varying distribution, record the time every so often
        # so we can pick up where we left off

        if opts.drift_time == -1:
            simulated_time += 1
        if simulated_time % time_save_rate == 0:
            with open(simtime_pathname, 'w') as time_fd:
                time_fd.write('%10d' % simulated_time)

    else:
        index = 'invalid-distribution-type'  # should never happen
    if verbosity & 0x20:
        print('next file index %u out of %u' % (index, max_files_per_dir))
    dirpath = gen_random_dirname(index)
    fn = os.path.join(dirpath, 'f%09d' % index)
    if verbosity & 0x20:
        print('next pathname %s' % fn)
    return fn


def random_file_size():
    return random.randint(1, opts.max_file_size_kb * BYTES_PER_KB)


def random_record_size():
    return random.randint(1, opts.max_record_size_kb * BYTES_PER_KB)


def random_segment_size(filesz):
    if opts.fix_record_size_kb:
        segsize = 2*opts.fix_record_size_kb * BYTES_PER_KB
    else:
        segsize = 2*random_record_size()
    if segsize > filesz:
        segsize = filesz//7
    return segsize


def random_seek_offset(filesz):
    return random.randint(0, filesz)


def try_to_close(closefd, filename):
    if closefd != FD_UNDEFINED:
        try:
            os.close(closefd)
        except OSError as e:
            scallerr('close', filename, e)
            return False
    return True

def get_recsz():
    if opts.fix_record_size_kb:
        return opts.fix_record_size_kb * BYTES_PER_KB
    else:
        return random_record_size()

def read():
    global e_file_not_found, have_read, read_requests, read_bytes
    global time_before, time_after
    s = OK
    fd = FD_UNDEFINED
    fn = gen_random_fn()
    try:
        fd = os.open(fn, os.O_RDONLY)
        stinfo = os.fstat(fd)
        if verbosity & 0x4000:
            print('read file %s sz %u' % (fn, stinfo.st_size))
        total_read = 0
        time_before = time.time()
        while total_read < stinfo.st_size:
            rdsz = random_record_size()
            bytes = os.read(fd, rdsz)
            count = len(bytes)
            read_requests += 1
            read_bytes += count
            if verbosity & 0x4000:
                print('seq. read off %u sz %u got %u' %\
                    (total_read, rdsz, count))
            total_read += len(bytes)
        time_after = time.time()
        have_read += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
        else:
            scallerr('close', filename, e)
            s = NOTOK
    try_to_close(fd, fn)
    return s


def random_read():
    global e_file_not_found, have_randomly_read, randread_requests, randread_bytes
    global time_before, time_after
    s = OK
    fd = FD_UNDEFINED
    have_randomly_read += 1
    fn = gen_random_fn()
    try:
        fd = os.open(fn, os.O_RDONLY)
        stinfo = os.fstat(fd)
        total_read_reqs = 0
        target_read_reqs = random.randint(1, opts.max_random_reads)
        if verbosity & 0x2000:
            print('randread %s filesize %u reqs %u' % (
                fn, stinfo.st_size, target_read_reqs))
        time_before = time.time()
        while total_read_reqs < target_read_reqs:
            off = os.lseek(fd, random_seek_offset(stinfo.st_size), 0)
            if verbosity & 0x2000:
                print('randread off %u sz %u' % (off, rdsz))            
            total_count = 0
            remaining_sz = stinfo.st_size - off
            targetsz = random_segment_size(stinfo.st_size)
            if opts.singleIO:
                targetsz = get_recsz()
            while total_count < targetsz:
                recsz = get_recsz()
                if recsz + total_count > remaining_sz:
                    recsz = remaining_sz - total_count
                elif recsz + total_count > targetsz:
                    recsz = targetsz - total_count
                if recsz == 0:
                    break
                bytebuf = os.read(fd, recsz)
                count = len(bytebuf)
                assert count > 0
                if verbosity & 0x2000:
                    print('randread recsz %u count %u' % (recsz, count))
                total_count += count
                randread_bytes += count
            total_read_reqs += 1
            randread_requests += 1
        time_after = time.time()
        have_randomly_read += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
        else:
            scallerr('random_read', fn, e)
            s = NOTOK
    try_to_close(fd, fn)
    return s


def maybe_fsync(fd):
    global fsyncs, fdatasyncs
    percent = random.randint(0, 100)
    if percent > opts.fsync_probability_pct + opts.fdatasync_probability_pct:
        return
    if percent > opts.fsync_probability_pct:
        fdatasyncs += 1
        os.fdatasync(fd)
    else:
        fsyncs += 1
        os.fsync(fd)


def create():
    global have_created, e_already_exists, write_requests, write_bytes, dirs_created
    global e_no_dir_space, e_no_inode_space, e_no_space
    global time_before, time_after
    s = OK
    fd = FD_UNDEFINED
    fn = gen_random_fn(is_create=True)
    target_sz = random_file_size()
    refresh_buf(target_sz)
    if verbosity & 0x1000:
        print('create %s sz %s' % (fn, target_sz))
    subdir = os.path.dirname(fn)
    if not os.path.isdir(subdir):
        try:
            os.makedirs(subdir)
        except OSError as e:
            if e.errno == errno.ENOSPC:
                e_no_dir_space += 1
                return OK
            scallerr('create', fn, e)
            return NOTOK
        dirs_created += 1
    try:
        fd = os.open(fn, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        total_sz = 0
        offset = 0
        time_before = time.time()
        while total_sz < target_sz:
            recsz = random_record_size()
            if recsz + total_sz > target_sz:
                recsz = target_sz - total_sz
            count = os.write(fd, buf[offset:offset+recsz])
            offset += count
            assert count > 0
            if verbosity & 0x1000:
                print('create sz %u written %u' % (recsz, count))
            total_sz += count
            write_requests += 1
            write_bytes += count
        maybe_fsync(fd)
        time_after = time.time()
        have_created += 1
    except os.error as e:
        if e.errno == errno.EEXIST:
            e_already_exists += 1
        elif e.errno == errno.ENOSPC:
            e_no_inode_space += 1
        else:
            scallerr('create', fn, e)
            s = NOTOK
    try_to_close(fd, fn)
    return s


def append():
    global have_appended, write_requests, write_bytes, e_file_not_found
    global e_no_space
    global time_before, time_after
    s = OK
    fn = gen_random_fn()
    target_sz = random_file_size()
    refresh_buf(target_sz)
    if verbosity & 0x8000:
        print('append %s sz %s' % (fn, target_sz))
    fd = FD_UNDEFINED
    try:
        fd = os.open(fn, os.O_WRONLY)
        have_appended += 1
        total_appended = 0
        offset = 0
        time_before = time.time()
        while total_appended < target_sz:
            recsz = random_record_size()
            if recsz + total_appended > target_sz:
                recsz = target_sz - total_appended
            assert recsz > 0
            if verbosity & 0x8000:
                print('append rsz %u' % (recsz))
            count = os.write(fd, buf[offset:offset+recsz])
            offset += count

            assert count > 0
            total_appended += count
            write_requests += 1
            write_bytes += count
        maybe_fsync(fd)
        time_after = time.time()
        have_appended += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
        elif e.errno == errno.ENOSPC:
            e_no_space += 1
        else:
            scallerr('append', fn, e)
            s = NOTOK
    try_to_close(fd, fn)
    return s


def random_write():
    global have_randomly_written, randwrite_requests, randwrite_bytes, e_file_not_found
    global e_no_space
    global time_before, time_after
    s = OK
    fd = FD_UNDEFINED
    fn = gen_random_fn()
    try:
        fd = os.open(fn, os.O_WRONLY)
        stinfo = os.fstat(fd)
        total_write_reqs = 0
        target_write_reqs = random.randint(1, opts.max_random_writes)
        if verbosity & 0x20000:
            print('randwrite %s reqs %u' % (fn, target_write_reqs))
        time_before = time.time()
        while total_write_reqs < target_write_reqs:
            off = os.lseek(fd, random_seek_offset(stinfo.st_size), 0)
            total_count = 0
            targetsz = random_segment_size(stinfo.st_size)
            if opts.singleIO:
                targetsz = get_recsz() 
            if verbosity & 0x20000:
                print('randwrite off %u sz %u' % (off, targetsz))
            while total_count < targetsz:
                recsz = get_recsz()
                if recsz + total_count > targetsz:
                    recsz = targetsz - total_count
                count = os.write(fd, buf[0:recsz])
                if verbosity & 0x20000:
                    print('randwrite count=%u recsz=%u' % (count, recsz))
                assert count > 0
                total_count += count
            total_write_reqs += 1
            randwrite_requests += 1
            randwrite_bytes += total_count
        maybe_fsync(fd)
        time_after = time.time()
        have_randomly_written += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
        elif e.errno == errno.ENOSPC:
            e_no_space += 1
        else:
            scallerr('random write', fn, e)
            s = NOTOK
    try_to_close(fd, fn)
    return s


def truncate():
    global have_truncated, e_file_not_found
    global time_before, time_after
    fd = FD_UNDEFINED
    s = OK
    fn = gen_random_fn()
    if verbosity & 0x40000:
        print('truncate %s' % fn)
    try:
        new_file_size = random_file_size()/3
        time_before = time.time()
        fd = os.open(fn, os.O_RDWR)
        os.ftruncate(fd, new_file_size)
        time_after = time.time()
        have_truncated += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
        else:
            scallerr('truncate', fn, e)
            s = NOTOK
    try_to_close(fd, fn)
    return s


def link():
    global have_linked, e_file_not_found, e_already_exists
    global time_before, time_after
    fn = gen_random_fn()
    fn2 = gen_random_fn() + link_suffix
    if verbosity & 0x10000:
        print('link to %s from %s' % (fn, fn2))
    if not os.path.isfile(fn):
        e_file_not_found += 1
        return OK
    try:
        time_before = time.time()
        rc = os.symlink(fn, fn2)
        time_after = time.time()
        have_linked += 1
    except os.error as e:
        if e.errno == errno.EEXIST:
            e_already_exists += 1
            return OK
        elif e.errno == errno.ENOENT:
            e_file_not_found += 1
            return OK
        scallerr('link', fn, e)
        return NOTOK
    return OK


def hlink():
    global have_hlinked, e_file_not_found, e_already_exists
    global time_before, time_after
    fn = gen_random_fn()
    fn2 = gen_random_fn() + hlink_suffix
    if verbosity & 0x10000:
        print('hard link to %s from %s' % (fn, fn2))
    if not os.path.isfile(fn):
        e_file_not_found += 1
        return OK
    try:
        time_before = time.time()
        rc = os.link(fn, fn2)
        time_after = time.time()
        have_hlinked += 1
    except os.error as e:
        if e.errno == errno.EEXIST:
            e_already_exists += 1
            return OK
        elif e.errno == errno.ENOENT:
            e_file_not_found += 1
            return OK
        scallerr('link', fn, e)
        return NOTOK
    return OK


def delete():
    global have_deleted, e_file_not_found
    global time_before, time_after
    fn = gen_random_fn()
    if verbosity & 0x20000:
        print('delete %s' % (fn))
    try:
        linkfn = fn + link_suffix
        time_before = time.time()
        if os.path.isfile(linkfn):
            if verbosity & 0x20000:
                print('delete soft link %s' % (linkfn))
            os.unlink(linkfn)
        hlinkfn = fn + hlink_suffix
        if os.path.isfile(hlinkfn):
            if verbosity & 0x20000:
                print('delete hard link %s' % (hlinkfn))
            os.unlink(hlinkfn)
        os.unlink(fn)
        time_after = time.time()
        have_deleted += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
            return OK
        scallerr('delete', fn, e)
        return NOTOK
    return OK


def rename():
    global have_renamed, e_file_not_found
    global time_before, time_after
    fn = gen_random_fn()
    fn2 = gen_random_fn()
    if verbosity & 0x20000:
        print('rename %s to %s' % (fn, fn2))
    try:
        time_before = time.time()
        os.rename(fn, fn2)
        time_after = time.time()
        have_renamed += 1
    except os.error as e:
        if e.errno == errno.ENOENT:
            e_file_not_found += 1
            return OK
        scallerr('rename', fn, e)
        return NOTOK
    return OK


rq_map = \
    {rq.READ: (read, "read"),
     rq.RANDOM_READ: (random_read, "random_read"),
     rq.CREATE: (create, "create"),
     rq.RANDOM_WRITE: (random_write, "random_write"),
     rq.APPEND: (append, "append"),
     rq.LINK: (link, "link"),
     rq.DELETE: (delete, "delete"),
     rq.RENAME: (rename, "rename"),
     rq.TRUNCATE: (truncate, "truncate"),
     rq.HARDLINK: (hlink, "hardlink")
     }


if __name__ == "__main__":
    opts.parseopts()
    buckets = 20
    histogram = [0 for x in range(0, buckets)]
    with open('/tmp/filenames.list', 'w') as filenames:
        for i in range(0, opts.opcount):
            fn = gen_random_fn()
            filenames.write(fn + '\n')
            # print(fn)
            namelist = fn.split('/')
            fname = namelist[len(namelist)-1].split('.')[0]
            # print(fname)
            num = int(fname[1:])
            bucket = num*len(histogram)/opts.max_files
            histogram[bucket] += 1
    print(histogram)
    assert(sum(histogram) == opts.opcount)
