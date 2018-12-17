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
from common import rq, FileAccessDistr, FileSizeDistr
from common import OK, NOTOK, BYTES_PER_KiB, FD_UNDEFINED, FsDriftException
from fsop_counters import FSOPCounters

link_suffix = '.s'
hlink_suffix = '.h'
rename_suffix = '.r'

large_prime = 12373

class FSOPCtx:

    opname_to_opcode = {
         "read":            rq.READ,
         "random_read":     rq.RANDOM_READ,
         "create":          rq.CREATE,
         "random_write":    rq.RANDOM_WRITE,
         "append":          rq.APPEND,
         "softlink":        rq.SOFTLINK,
         "hardlink":        rq.HARDLINK,
         "delete":          rq.DELETE,
         "rename":          rq.RENAME,
         "truncate":        rq.TRUNCATE,
         "remount":         rq.REMOUNT
    }
    
    opcode_to_opname = {
         rq.READ:           "read",
         rq.RANDOM_READ:    "random_read",
         rq.CREATE:         "create",
         rq.RANDOM_WRITE:   "random_write",
         rq.APPEND:         "append",
         rq.SOFTLINK:       "softlink",
         rq.HARDLINK:       "hardlink",
         rq.DELETE:         "delete",
         rq.RENAME:         "rename",
         rq.TRUNCATE:       "truncate",
         rq.REMOUNT:        "remount"
    }

    # for gaussian distribution with moving mean, we need to remember simulated time
    # so we can pick up where we left off with moving mean

    simtime_filename = 'fs-drift-simtime.tmp'
    SIMULATED_TIME_UNDEFINED = None
    time_save_rate = 5

    def __init__(self, params, log, ctrs):
        self.ctrs = ctrs
        self.params = params
        self.log = log
        self.buf = random_buffer.gen_buffer(params.max_record_size_kb*BYTES_PER_KiB)
        self.total_dirs = 1
        self.verbosity = self.params.verbosity 
        for i in range(0, self.params.levels):
            self.total_dirs *= self.params.subdirs_per_dir
        # most recent center
        self.last_center = 0
        self.simulated_time = FSOPCtx.SIMULATED_TIME_UNDEFINED  # initialized later
        self._rqmap = {
            rq.READ:        self.op_read,
            rq.RANDOM_READ: self.op_random_read,
            rq.CREATE:      self.op_create,
            rq.RANDOM_WRITE: self.op_random_write,
            rq.APPEND:      self.op_append,
            rq.SOFTLINK:    self.op_softlink,
            rq.HARDLINK:    self.op_hardlink,
            rq.DELETE:      self.op_delete,
            rq.RENAME:      self.op_rename,
            rq.TRUNCATE:    self.op_truncate,
            rq.REMOUNT:     self.op_remount,
            }

    # clients invoke functions by workload request type code
    # instead of by function name, using this:

    def invoke_rq(self, rqcode):
        return self._rqmap[rqcode]()

    def scallerr(self, msg, fn, syscall_exception):
        self.log.exception(syscall_exception)
        try:
            err = syscall_exception.errno
            self.log.error('%s: %s syscall errno %d(%s)' % (
                            msg, fn, err, os.strerror(err)))
        except Exception:
            self.log.error('non-OSError exception %s: %s' % (msg, fn))

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
            if self.verbosity & 0x20:
                self.log.debug('%f = center' % center)
            index_float = numpy.random.normal(
                loc=center, scale=self.params.gaussian_stddev)
            file_opstr = 'read'
            if is_create:
                file_opstr = 'create'
            if self.verbosity & 0x20:
                self.log.debug('%s gaussian value is %f' % (file_opstr, index_float))
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
        return random.randint(0, self.params.max_file_size_kb * BYTES_PER_KiB)


    def random_record_size(self):
        return random.randint(1, self.params.max_record_size_kb * BYTES_PER_KiB)


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
        except OSError as e:
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
                    if self.verbosity & 0x2000:
                        self.log.debug('randread recsz %u count %u' % (recsz, count))
                    total_count += count
                    c.randread_bytes += count
                total_read_reqs += 1
                c.randread_requests += 1
            c.have_randomly_read += 1
        except OSError as e:
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
            return OK
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
        except OSError as e:
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
                if self.verbosity & 0x8000:
                    self.log.debug('append rsz %u' % (recsz))
                count = os.write(fd, self.buf[0:recsz])
                assert count == recsz
                total_appended += count
                c.write_requests += 1
                c.write_bytes += count
            rc = self.maybe_fsync(fd)
            c.have_appended += 1
        except OSError as e:
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
                    if self.verbosity & 0x20000:
                        self.log.debug('randwrite count=%u recsz=%u' % (count, recsz))
                    assert count == recsz
                    total_count += count
                total_write_reqs += 1
                c.randwrite_requests += 1
                c.randwrite_bytes += total_count
                rc = self.maybe_fsync(fd)
            c.have_randomly_written += 1
        except OSError as e:
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
        if self.verbosity & 0x40000:
            self.log.debug('truncate %s' % fn)
        try:
            new_file_size = self.random_file_size()/3
            fd = os.open(fn, os.O_RDWR)
            s = os.ftruncate(fd, new_file_size)
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
        except OSError as e:
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
        except OSError as e:
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
        except OSError as e:
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
            raise FsDriftException('you did not specify mount command for remount option')
        if self.verbosity & 0x40000:
            self.log.debug('remount: %s' % self.params.mount_command)
        mountpoint = self.params.mount_command.split()[-1].strip()
        if not self.params.top_directory.startswith(mountpoint):
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
            rc = os.system('umount %s' % mountpoint)
            if rc != OK:
                c.e_could_not_unmount += 1
                return rc
        rc = os.system(self.params.mount_command)
        if rc != OK:
            c.e_could_not_mount += 1
            return rc
        c.have_remounted += 1
        return OK


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
    ctrs = FSOPCounters()
    ctx = FSOPCtx(options, log, ctrs)
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
    #rc = ctx.op_remount()
    #assert(rc != OK)

    # simulate a mixed-workload run
    for j in range(0, 200):
        for k in FSOPCtx.opcode_to_opname.keys():
            if k != rq.REMOUNT:
                rc = ctx.invoke_rq(k)
            assert(rc == OK)

    # output FSOPCounter object
    print(ctrs)
    ctrs2 = FSOPCounters()
    ctrs.add_to(ctrs2)
    ctrs.add_to(ctrs2)
    assert(ctrs2.have_read > 0 and ctrs2.have_read == 2 * ctrs.have_read)
    print(ctrs.json_dict())

