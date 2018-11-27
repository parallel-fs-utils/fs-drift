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
import counters

link_suffix = '.s'
hlink_suffix = '.h'
rename_suffix = '.r'

large_prime = 12373

# for gaussian distribution with moving mean, we need to remember simulated time
# so we can pick up where we left off with moving mean


class FSOPCtx:
    simtime_filename = 'fs-drift-simtime.tmp'
    SIMULATED_TIME_UNDEFINED = None
    time_save_rate = 5

    def __init__(self, params, log):
        self.ctrs = counters.FSOPCounters()
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
        a = subprocess.Popen("date", shell=True,
                             stdout=subprocess.PIPE).stdout.read()
        self.log.error('%s: %s: %s syscall errno %d(%s)' % (
            a.rstrip('\n'), msg, fn, err, os.strerror(err)))

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

    def read(self):
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
                scallerr('close', filename, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s

    def random_read(self):
        global e_file_not_found, have_randomly_read, randread_requests, randread_bytes
        c = self.ctrs
        s = OK
        fd = FD_UNDEFINED
        c.have_randomly_read += 1
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
                    assert count > 0
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
                scallerr('random_read', fn, e)
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

    def create(self):
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
                scallerr('create', fn, e)
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
                assert count > 0
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


    def append(self):
        c = self.ctrs
        s = OK
        fn = self.gen_random_fn()
        target_sz = self.random_file_size()
        if self.verbosity & 0x8000:
            self.log.debug('append %s sz %s' % (fn, target_sz))
        fd = FD_UNDEFINED
        try:
            fd = os.open(fn, os.O_WRONLY)
            c.have_appended += 1
            total_appended = 0
            while total_appended < target_sz:
                recsz = self.random_record_size()
                if recsz + total_appended > target_sz:
                    recsz = target_sz - total_appended
                assert recsz > 0
                if verbosity & 0x8000:
                    print('append rsz %u' % (recsz))
                count = os.write(fd, self.buf[0:recsz])
                assert count > 0
                total_appended += count
                c.write_requests += 1
                c.write_bytes += count
            rc = self.maybe_fsync(fd)
        except os.error as e:
            if e.errno == errno.ENOENT:
                c.e_file_not_found += 1
            elif e.errno == errno.ENOSPC:
                c.e_no_space += 1
            else:
                scallerr('append', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def random_write(self):
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
                    assert count > 0
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
                scallerr('random write', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def truncate(self):
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
                scallerr('truncate', fn, e)
                s = NOTOK
        self.try_to_close(fd, fn)
        return s


    def link(self):
        c = self.ctrs
        fn = self.gen_random_fn()
        fn2 = self.gen_random_fn() + link_suffix
        if self.verbosity & 0x10000:
            self.log.debug('link to %s from %s' % (fn, fn2))
        if not os.path.isfile(fn):
            c.e_file_not_found += 1
            return OK
        try:
            rc = os.symlink(fn, fn2)
            c.have_linked += 1
        except os.error as e:
            if e.errno == errno.EEXIST:
                c.e_already_exists += 1
                return OK
            elif e.errno == errno.ENOENT:
                c.e_file_not_found += 1
                return OK
            scallerr('link', fn, e)
            return NOTOK
        return OK


    def hlink(self):
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
            c.have_hlinked += 1
        except os.error as e:
            if e.errno == errno.EEXIST:
                c.e_already_exists += 1
                return OK
            elif e.errno == errno.ENOENT:
                c.e_file_not_found += 1
                return OK
            scallerr('link', fn, e)
            return NOTOK
        return OK


    def delete(self):
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
            scallerr('delete', fn, e)
            return NOTOK
        return OK


    def rename(self):
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
            scallerr('rename', fn, e)
            return NOTOK
        return OK


    # unmounting is so risky that we shouldn't try to figure it out
    # make the user tell us the entire mount command
    # we will get mountpoint from last token on the command line
    # assumption: mountpoint comes last on the mount command
    
    def remount(self):
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


    def gen_rq_map(self):
        
        return {
         rq.READ: (self.read, "read"),
         rq.RANDOM_READ: (self.random_read, "random_read"),
         rq.CREATE: (self.create, "create"),
         rq.RANDOM_WRITE: (self.random_write, "random_write"),
         rq.APPEND: (self.append, "append"),
         rq.LINK: (self.link, "link"),
         rq.DELETE: (self.delete, "delete"),
         rq.RENAME: (self.rename, "rename"),
         rq.TRUNCATE: (self.truncate, "truncate"),
         rq.HARDLINK: (self.hlink, "hardlink"),
         rq.REMOUNT: (self.remount, "remount")
         }


def start_log():
    log = logging.getLogger('fsop-unittest')
    h = logging.StreamHandler()
    log_format = ('fsop-unittest %(asctime)s - %(levelname)s - %(message)s')
    formatter = logging.Formatter(log_format)
    h.setFormatter(formatter)
    log.addHandler(h)
    log.setLevel(logging.DEBUG)
    return log

if __name__ == "__main__":
    import logging
    import opts
    options = opts.parseopts()
    log = start_log()
    log.info('hi there')
    if not options.top_directory.__contains__('/tmp/'):
        raise FSDriftException('bad top directory')
    os.system('rm -rf %s' % options.top_directory)
    os.makedirs(options.top_directory)
    os.chdir(options.top_directory)
    log.info('chdir to %s' % options.top_directory)
    ctx = FSOPCtx(options, log)
    rc = ctx.create()
    assert(rc == OK)
    rc = ctx.read()
    assert(rc == OK)
    rc = ctx.random_read()
    assert(rc == OK)
    rc = ctx.append()
    assert(rc == OK)
    rc = ctx.random_write()
    assert(rc == OK)
    rc = ctx.truncate()
    assert(rc == OK)
    rc = ctx.link()
    assert(rc == OK)
    rc = ctx.hlink()
    assert(rc == OK)
    rc = ctx.delete()
    assert(rc == OK)
    rc = ctx.rename()
    assert(rc == OK)
    rc = ctx.remount()
    assert(rc != OK)
    map = ctx.gen_rq_map()
    oplist = [ rq.CREATE, rq.READ, rq.RANDOM_READ, rq.RANDOM_WRITE, rq.APPEND, 
                rq.LINK, rq.DELETE, rq.RENAME, rq.TRUNCATE, rq.HARDLINK, rq.REMOUNT ]
    for j in range(0, 200):
        for k in oplist:
            (func, name) = map[oplist[k]]
            rc = func()
            assert(rc == OK or oplist[k] == rq.REMOUNT)
