# fsop.py - module containing filesystem operation types and common code for them

# NOTE: this version requires "numpy" rpm to be installed
# std python modules
import os
import os.path
import random
import errno
import random_buffer
import numpy  # for gaussian distribution
import subprocess

# my modules
import common
from common import rq, FileAccessDistr, FileSizeDistr, verbosity
from common import OK, NOTOK, BYTES_PER_KB, FD_UNDEFINED

link_suffix = '.s'
hlink_suffix = '.h'
rename_suffix = '.r'

large_prime = 12373

# define class for counters so we can 
# easily export them or convert them to JSON

class FSOPCounters:

    def __init__(self):
        # operation counters, incremented by op function below
        self.have_created = 0
        self.have_deleted = 0
        self.have_softlinked = 0
        self.have_hardlinked = 0
        self.have_appended = 0
        self.have_randomly_written = 0
        self.have_read = 0
        self.have_randomly_read = 0
        self.have_renamed = 0
        self.have_truncated = 0
        self.have_remounted = 0
        
        # throughput counters
        self.read_requests = 0
        self.read_bytes = 0
        self.randread_requests = 0
        self.randread_bytes = 0
        self.write_requests = 0
        self.write_bytes = 0
        self.randwrite_requests = 0
        self.randwrite_bytes = 0
        self.fsyncs = 0
        self.fdatasyncs = 0
        self.dirs_created = 0
        
        # error counters
        self.e_already_exists = 0
        self.e_file_not_found = 0
        self.e_no_dir_space = 0
        self.e_no_inode_space = 0
        self.e_no_space = 0
        self.e_not_mounted = 0
        self.e_could_not_mount = 0
        
    def kvtuplelist(self):
        return [
            ('created', self.have_created),
            ('deleted', self.have_deleted),
            ('softlinked', self.have_softlinked),
            ('hardlinked', self.have_hardlinked),
            ('appended', self.have_appended),
            ('randomly_written', self.have_randomly_written),
            ('read', self.have_read),
            ('randomly_read', self.have_randomly_read),
            ('renamed', self.have_renamed),
            ('truncated', self.have_truncated),
            ('remounted', self.have_remounted),
            ('read_requests', self.read_requests),
            ('read_bytes', self.read_bytes),
            ('randread_requests', self.randread_requests),
            ('randread_bytes', self.randread_bytes),
            ('write_requests', self.write_requests),
            ('write_bytes', self.write_bytes),
            ('randwrite_requests', self.randwrite_requests),
            ('randwrite_bytes', self.randwrite_bytes),
            ('fsyncs', self.fsyncs),
            ('fdatasyncs', self.fdatasyncs),
            ('dirs_created', self.dirs_created),
            ('e_already_exists', self.e_already_exists),
            ('e_file_not_found', self.e_file_not_found),
            ('e_no_dir_space', self.e_no_dir_space),
            ('e_no_inode_space', self.e_no_inode_space),
            ('e_no_space', self.e_no_space),
            ('e_not_mounted', self.e_not_mounted),
            ('e_could_not_mount', self.e_could_not_mount)
            ]

    def __str__(self):
        field_list = [ '%20s = %d' % f for f in self.kvtuplelist() ]
        return '\n'.join(field_list)

    def json_dict(self):
        d = {}
        for (k, v) in self.kvtuplelist():
            d[k] = v
        return d

class FSOPCtx:

    # for gaussian distribution with moving mean, we need to remember simulated time
    # so we can pick up where we left off with moving mean

    simtime_filename = 'fs-drift-simtime.tmp'
    SIMULATED_TIME_UNDEFINED = None
    time_save_rate = 5

    def __init__(self, params, log):
        self.ctrs = FSOPCounters()
        self.params = params
        self.log = log
        self.buf = random_buffer.gen_buffer(params.max_record_size_kb*BYTES_PER_KB)
        self.total_dirs = 1
        self.verbosity = -1
        for i in range(0, self.params.levels):
            self.total_dirs *= self.params.subdirs_per_dir
        # most recent center
        self.last_center = 0
        self.simulated_time = FSOPCtx.SIMULATED_TIME_UNDEFINED  # initialized later

    def scallerr(self, msg, fn, syscall_exception):
        err = syscall_exception.errno
        self.log.error('%s: %s syscall errno %d(%s)' % (
            msg, fn, err, os.strerror(err)))
        self.log.exception(syscall_exception)

    def gen_random_dirname(self, file_index):
        subdirs_per_dir = self.params.subdirs_per_dir
        d = '.'
        # multiply file_index ( < opts.max_files) by large number relatively prime to subdirs_per_dir
        index = file_index * large_prime
        for j in range(0, self.params.levels):
            subdir_index = 1 + (index % subdirs_per_dir)
            dname = 'd%04d' % subdir_index
            d = os.path.join(d, dname)
            index /= subdirs_per_dir
        return d


    def gen_random_fn(self, is_create=False):
        max_files_per_dir = self.params.max_files // self.total_dirs
    
        if self.params.rand_distr_type == FileAccessDistr.uniform:
            # lower limit 0 means at least 1 file/dir
            index = random.randint(0, max_files_per_dir)
        elif self.params.rand_distr_type == FileAccessDistr.gaussian:
    
            # if simulated time is not defined,
            # attempt to read it in from a file, set to zero if no file
    
            if simulated_time == SIMULATED_TIME_UNDEFINED:
                try:
                    simtime_pathname = os.path.join(self.params.network_shared_path, simtime_filename)
                    with open(simtime_pathname, 'r') as readtime_fd:
                        simulated_time = int(readtime_fd.readline().strip())
                except IOError as e:
                    if e.errno != errno.ENOENT:
                        raise e
                    simulated_time = 0
                self.log(('resuming with simulated time %d' % simulated_time))
    
            # for creates, use greater time, so that reads, etc. will "follow" creates most of the time
            # mean and std deviation define gaussian distribution
    
            center = (simulated_time * self.params.mean_index_velocity)
            if is_create:
                center += (self.params.create_stddevs_ahead * self.params.gaussian_stddev)
            if verbosity & 0x20:
                print('%f = center' % center)
            index_float = numpy.random.normal(
                loc=center, scale=self.params.gaussian_stddev)
            file_opstr = 'read'
            if is_create:
                file_opstr = 'create'
            if verbosity & 0x20:
                print('%s gaussian value is %f' % (file_opstr, index_float))
            #index = int(index_float) % max_files_per_dir
            index = int(index_float) % self.params.max_files
            last_center = center
    
            # since this is a time-varying distribution, record the time every so often
            # so we can pick up where we left off
    
            if self.params.drift_time == -1:
                simulated_time += 1
            if simulated_time % time_save_rate == 0:
                with open(simtime_pathname, 'w') as time_fd:
                    time_fd.write('%10d' % simulated_time)
    
        else:
            index = 'invalid-distribution-type'  # should never happen
        if self.verbosity & 0x20:
            self.log.debug('next file index %u out of %u' % (index, max_files_per_dir))
        dirpath = self.gen_random_dirname(index)
        fn = os.path.join(dirpath, 'f%09d' % index)
        if self.verbosity & 0x20:
            self.log.debug('next pathname %s' % fn)
        return fn


    def random_file_size(self):
        return random.randint(0, self.params.max_file_size_kb * BYTES_PER_KB)


    def random_record_size(self):
        return random.randint(1, self.params.max_record_size_kb * BYTES_PER_KB)


    def random_segment_size(self, filesz):
        segsize = 2 * self.random_record_size()
        if segsize > filesz:
            segsize = filesz//7
        return segsize

    def random_seek_offset(self, filesz):
        return random.randint(0, filesz)


    def try_to_close(self, closefd, filename):
        if closefd != FD_UNDEFINED:
            try:
                os.close(closefd)
            except OSError as e:
                self.scallerr('close', filename, e)
                return False
        return True

    def op_read(self):
        c = self.ctrs
        s = OK
        fd = FD_UNDEFINED
        fn = self.gen_random_fn()
        try:
            if self.verbosity & 0x20000:
                self.log.debug('read file %s' % fn)
            fd = os.open(fn, os.O_RDONLY)
            stinfo = os.fstat(fd)
            if self.verbosity & 0x4000:
                self.log.debug('read file sz %u' % (stinfo.st_size))
            total_read = 0
            while total_read < stinfo.st_size:
                rdsz = self.random_record_size()
                bytes = os.read(fd, rdsz)
                count = len(bytes)
                if count < 1:
                    break
                c.read_requests += 1
                c.read_bytes += count
                if self.verbosity & 0x4000:
                    self.log.debug('seq. read off %u sz %u got %u' %\
                        (total_read, rdsz, count))
                total_read += len(bytes)
            c.have_read += 1
        except os.error as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
            else:
                self.scallerr('close', filename, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s

    def op_random_read(self):
        c = self.ctrs
        s = OK
        fd = FD_UNDEFINED
        fn = self.gen_random_fn()
        try:
            total_read_reqs = 0
            target_read_reqs = random.randint(0, self.params.max_random_reads)
            if self.verbosity & 0x20000:
                self.log.debug('randread %s reqs %u' % (fn, target_read_reqs))
            fd = os.open(fn, os.O_RDONLY)
            stinfo = os.fstat(fd)
            if self.verbosity & 0x2000:
                self.log.debug('randread filesize %u reqs %u' % (
                    stinfo.st_size, target_read_reqs))
            while total_read_reqs < target_read_reqs:
                off = os.lseek(fd, self.random_seek_offset(stinfo.st_size), 0)
                rdsz = self.random_segment_size(stinfo.st_size)
                if self.verbosity & 0x2000:
                    self.log.debug('randread off %u sz %u' % (off, rdsz))
                total_count = 0
                remaining_sz = stinfo.st_size - off
                while total_count < rdsz:
                    recsz = self.random_record_size()
                    if recsz + total_count > remaining_sz:
                        recsz = remaining_sz - total_count
                    elif recsz + total_count > rdsz:
                        recsz = rdsz - total_count
                    if recsz == 0:
                        break
                    bytebuf = os.read(fd, recsz)
                    count = len(bytebuf)
                    if count < 1:
                        break
                    if verbosity & 0x2000:
                        print('randread recsz %u count %u' % (recsz, count))
                    total_count += count
                    c.randread_bytes += count
                total_read_reqs += 1
                c.randread_requests += 1
            c.have_randomly_read += 1
        except os.error as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
            else:
                self.scallerr('random_read', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def maybe_fsync(self, fd):
        c = self.ctrs
        percent = random.randint(0, 100)
        if percent > self.params.fsync_probability_pct + self.params.fdatasync_probability_pct:
            return
        if percent > self.params.fsync_probability_pct:
            c.fdatasyncs += 1
            rc = os.fdatasync(fd)
        else:
            c.fsyncs += 1
            rc = os.fsync(fd)
        return rc

    def op_create(self):
        c = self.ctrs
        s = OK
        fd = FD_UNDEFINED
        fn = self.gen_random_fn(is_create=True)
        target_sz = self.random_file_size()
        if self.verbosity & 0x1000:
            self.log.debug('create %s sz %s' % (fn, target_sz))
        subdir = os.path.dirname(fn)
        if not os.path.isdir(subdir):
            try:
                os.makedirs(subdir)
            except OSError as e:
                if e.errno == errno.ENOSPC:
                    c.e_no_dir_space += 1
                    return OK
                elif e.errno != errno.EEXIST:
                    self.scallerr('create', fn, e)
                    return NOTOK
            c.dirs_created += 1
        try:
            fd = os.open(fn, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            total_sz = 0
            while total_sz < target_sz:
                recsz = self.random_record_size()
                if recsz + total_sz > target_sz:
                    recsz = target_sz - total_sz
                count = os.write(fd, self.buf[0:recsz])
                assert count == recsz 
                if self.verbosity & 0x1000:
                    self.log.debug('create sz %u written %u' % (recsz, count))
                total_sz += count
                c.write_requests += 1
                c.write_bytes += count
            rc = self.maybe_fsync(fd)
            c.have_created += 1
        except os.error as e:
            if e.errno == errno.EEXIST:
                c.e_already_exists += 1
            elif e.errno == errno.ENOSPC:
                c.e_no_inode_space += 1
            else:
                self.scallerr('create', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def op_append(self):
        c = self.ctrs
        s = OK
        fn = self.gen_random_fn()
        target_sz = self.random_file_size()
        if self.verbosity & 0x8000:
            self.log.debug('append %s sz %s' % (fn, target_sz))
        fd = FD_UNDEFINED
        try:
            fd = os.open(fn, os.O_WRONLY)
            total_appended = 0
            while total_appended < target_sz:
                recsz = self.random_record_size()
                if recsz + total_appended > target_sz:
                    recsz = target_sz - total_appended
                assert recsz > 0
                if verbosity & 0x8000:
                    print('append rsz %u' % (recsz))
                count = os.write(fd, self.buf[0:recsz])
                assert count == recsz
                total_appended += count
                c.write_requests += 1
                c.write_bytes += count
            rc = self.maybe_fsync(fd)
            c.have_appended += 1
        except os.error as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
            elif e.errno == errno.ENOSPC:
                c.e_no_space += 1
            else:
                self.scallerr('append', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def op_random_write(self):
        c = self.ctrs
        s = OK
        fd = FD_UNDEFINED
        fn = self.gen_random_fn()
        try:
            total_write_reqs = 0
            target_write_reqs = random.randint(0, self.params.max_random_writes)
            if self.verbosity & 0x20000:
                self.log.debug('randwrite %s reqs %u' % (fn, target_write_reqs))
            fd = os.open(fn, os.O_WRONLY)
            stinfo = os.fstat(fd)
            while total_write_reqs < target_write_reqs:
                off = os.lseek(fd, self.random_seek_offset(stinfo.st_size), 0)
                total_count = 0
                wrsz = self.random_segment_size(stinfo.st_size)
                if self.verbosity & 0x20000:
                    self.log.debug('randwrite off %u sz %u' % (off, wrsz))
                while total_count < wrsz:
                    recsz = self.random_record_size()
                    if recsz + total_count > wrsz:
                        recsz = wrsz - total_count
                    count = os.write(fd, self.buf[0:recsz])
                    if verbosity & 0x20000:
                        print('randwrite count=%u recsz=%u' % (count, recsz))
                    assert count == recsz
                    total_count += count
                total_write_reqs += 1
                c.randwrite_requests += 1
                c.randwrite_bytes += total_count
                rc = self.maybe_fsync(fd)
            c.have_randomly_written += 1
        except os.error as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
            elif e.errno == errno.ENOSPC:
                c.e_no_space += 1
            else:
                self.scallerr('random write', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def op_truncate(self):
        c = self.ctrs
        fd = FD_UNDEFINED
        s = OK
        fn = self.gen_random_fn()
        if verbosity & 0x40000:
            print('truncate %s' % fn)
        try:
            new_file_size = self.random_file_size()/3
            fd = os.open(fn, os.O_RDWR)
            os.ftruncate(fd, new_file_size)
            c.have_truncated += 1
        except OSError as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
            else:
                self.scallerr('truncate', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def op_softlink(self):
        c = self.ctrs
        fn = os.getcwd() + os.sep + self.gen_random_fn()
        fn2 = self.gen_random_fn() + link_suffix
        if self.verbosity & 0x10000:
            self.log.debug('link to %s from %s' % (fn, fn2))
        if not os.path.isfile(fn):
            c.e_file_not_found += 1
            return OK
        try:
            rc = os.symlink(fn, fn2)
            c.have_softlinked += 1
        except os.error as e:
            if e.errno == errno.EEXIST:
                c.e_already_exists += 1
                return OK
            elif e.errno == errno.ENOENT:
                c.e_file_not_found += 1
                return OK
            self.scallerr('link', fn, e)
            return NOTOK
        return OK


    def op_hardlink(self):
        c = self.ctrs
        fn = self.gen_random_fn()
        fn2 = self.gen_random_fn() + hlink_suffix
        if self.verbosity & 0x10000:
            self.log.debug('hard link to %s from %s' % (fn, fn2))
        if not os.path.isfile(fn):
            c.e_file_not_found += 1
            return OK
        try:
            rc = os.link(fn, fn2)
            c.have_hardlinked += 1
        except os.error as e:
            if e.errno == errno.EEXIST:
                c.e_already_exists += 1
                return OK
            elif e.errno == errno.ENOENT:
                c.e_file_not_found += 1
                return OK
            self.scallerr('link', fn, e)
            return NOTOK
        return OK


    def op_delete(self):
        c = self.ctrs
        fn = self.gen_random_fn()
        if self.verbosity & 0x20000:
            self.log.debug('delete %s' % (fn))
        try:
            linkfn = fn + link_suffix
            if os.path.exists(linkfn):
                if self.verbosity & 0x20000:
                    self.log.debug('delete soft link %s' % (linkfn))
                os.unlink(linkfn)
            else:
                c.e_file_not_found += 1
            hlinkfn = fn + hlink_suffix
            if os.path.exists(hlinkfn):
                if self.verbosity & 0x20000:
                    self.log.debug('delete hard link %s' % (hlinkfn))
                os.unlink(hlinkfn)
            else:
                c.e_file_not_found += 1
            if self.verbosity & 0x20000:
                self.log.debug('delete file %s' % fn)
            os.unlink(fn)
            c.have_deleted += 1
        except OSError as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
                return OK
            self.scallerr('delete', fn, e)
            return NOTOK
        return OK


    def op_rename(self):
        c = self.ctrs
        fn = self.gen_random_fn()
        fn2 = self.gen_random_fn()
        if self.verbosity & 0x20000:
            self.log.debug('rename %s to %s' % (fn, fn2))
        try:
            os.rename(fn, fn2)
            c.have_renamed += 1
        except os.error as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
                return OK
            self.scallerr('rename', fn, e)
            return NOTOK
        return OK


    # unmounting is so risky that we shouldn't try to figure it out
    # make the user tell us the entire mount command
    # we will get mountpoint from last token on the command line
    # assumption: mountpoint comes last on the mount command
    
    def op_remount(self):
        c = self.ctrs
        if self.params.mount_command == None:
            self.log.warn('you did not specify mount command for remount option')
            return
        if self.verbosity & 0x40000:
            self.log.debug('remount: %s' % self.params.mount_command)
        mountpoint = self.params.mount_command.split()[-1].strip()
        if not self.params.topdir.startswith(mountpoint):
            raise common.FsDriftException(
                    'mountpoint %s does not contain topdir %s' % 
                    (mountpoint, topdir))
        with open('/proc/mounts', 'r') as mount_f:
            mounts = [ l.strip().split() for l in mount_f.readlines() ]
        mount_entry = None
        for m in mounts:
            if m[1] == mountpoint:
                mount_entry = m
                break
        if mount_entry == None:
            c.e_not_mounted += 1
        else:
            os.chdir('/tmp')
            rc = os.system('umount %s' % self.params.mountpoint)
            if rc != OK:
                c.e_could_not_unmount += 1
                return
        rc = os.system(self.params.mount_command)
        if rc != OK:
            c.e_could_not_mount += 1
            return

    # this is taking advantage of python closures to
    # allow passing class members as functions to call
    # without calling from a instance of that class

    def gen_rq_map(self):
        
        return {
         rq.READ: (self.op_read, "read"),
         rq.RANDOM_READ: (self.op_random_read, "random_read"),
         rq.CREATE: (self.op_create, "create"),
         rq.RANDOM_WRITE: (self.op_random_write, "random_write"),
         rq.APPEND: (self.op_append, "append"),
         rq.SOFTLINK: (self.op_softlink, "softlink"),
         rq.HARDLINK: (self.op_hardlink, "hardlink"),
         rq.DELETE: (self.op_delete, "delete"),
         rq.RENAME: (self.op_rename, "rename"),
         rq.TRUNCATE: (self.op_truncate, "truncate"),
         rq.REMOUNT: (self.op_remount, "remount")
         }


# unit test

if __name__ == "__main__":
    import logging
    import opts
    import fsd_log
    options = opts.parseopts()
    log = fsd_log.start_log('fsop-unittest')
    log.info('hi there')
    if not options.top_directory.__contains__('/tmp/'):
        raise FSDriftException('bad top directory')
    os.system('rm -rf %s' % options.top_directory)
    os.makedirs(options.top_directory)
    os.chdir(options.top_directory)
    log.info('chdir to %s' % options.top_directory)
    ctx = FSOPCtx(options, log)
    rc = ctx.op_create()
    assert(rc == OK)
    rc = ctx.op_read()
    assert(rc == OK)
    rc = ctx.op_random_read()
    assert(rc == OK)
    rc = ctx.op_append()
    assert(rc == OK)
    rc = ctx.op_random_write()
    assert(rc == OK)
    rc = ctx.op_truncate()
    assert(rc == OK)
    rc = ctx.op_softlink()
    assert(rc == OK)
    rc = ctx.op_hardlink()
    assert(rc == OK)
    rc = ctx.op_delete()
    assert(rc == OK)
    rc = ctx.op_rename()
    assert(rc == OK)
    rc = ctx.op_remount()
    assert(rc != OK)

    # output FSOPCounter object
    print(ctx.ctrs)
    print(ctx.ctrs.json_dict())

    # simulate a mixed-workload run
    rq_map = ctx.gen_rq_map()
    oplist = rq_map.keys()
    for j in range(0, 200):
        for k in oplist:
            (func, name) = rq_map[oplist[k]]
            rc = func()
            # remount not implemented yet
            assert(rc == OK or oplist[k] == rq.REMOUNT)
