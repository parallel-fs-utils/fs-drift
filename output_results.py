import time, sys
import fsop

# print out counters for the interval that just completed.

def print_short_stats(start_time):
    print('elapsed time: %9.1f' % (time.time() - start_time))
    print('\n'\
        '%9u = center\n' \
        '%9u = files created\t' \
        '%9u = files appended to\n' \
        '%9u = files random write\t' \
        '%9u = files read\n' \
        '%9u = files randomly read\n' \
        % (fsop.last_center, fsop.have_created, fsop.have_appended, fsop.have_randomly_written,
           fsop.have_read, fsop.have_randomly_read))
    sys.stdout.flush()


def print_stats(start_time, total_errors):
    print('')
    print('elapsed time: %9.1f' % (time.time() - start_time))
    print('\n\n'\
        '%9u = center\n' \
        '%9u = files created\n' \
        '%9u = files appended to\n' \
        '%9u = files randomly written to\n' \
        '%9u = files read\n' \
        '%9u = files randomly read\n' \
        '%9u = files truncated\n' \
        '%9u = files deleted\n' \
        '%9u = files renamed\n' \
        '%9u = softlinks created\n' \
        '%9u = hardlinks created\n' \
        % (fsop.last_center, fsop.have_created, fsop.have_appended, fsop.have_randomly_written,
           fsop.have_read, fsop.have_randomly_read, fsop.have_truncated,
           fsop.have_deleted, fsop.have_renamed, fsop.have_linked, fsop.have_hlinked))

    print('%9u = read requests\n' \
        '%9u = read bytes\n'\
        '%9u = random read requests\n' \
        '%9u = random read bytes\n' \
        '%9u = write requests\n' \
        '%9u = write bytes\n'\
        '%9u = random write requests\n' \
        '%9u = random write bytes\n' \
        '%9u = fdatasync calls\n' \
        '%9u = fsync calls\n' \
        '%9u = leaf directories created\n' \
        % (fsop.read_requests, fsop.read_bytes, fsop.randread_requests, fsop.randread_bytes,
           fsop.write_requests, fsop.write_bytes, fsop.randwrite_requests, fsop.randwrite_bytes,
           fsop.fdatasyncs, fsop.fsyncs, fsop.dirs_created))

    print('%9u = no create -- file already existed\n'\
        '%9u = file not found\n'\
        % (fsop.e_already_exists, fsop.e_file_not_found))
    print('%9u = no directory space\n'\
        '%9u = no space for new inode\n'\
        '%9u = no space for write data\n'\
        % (fsop.e_no_dir_space, fsop.e_no_inode_space, fsop.e_no_space))
    print('%9u = total errors' % total_errors)
    sys.stdout.flush()

