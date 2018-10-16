# opts.py - module to parse command line options and output what parameters will be used for this run

import os
import os.path
import sys
import common
from common import rq, file_access_dist, OK, NOTOK


def usage(msg):
    print(msg)
    print('usage: fs-drift.py [ --option value ]')
    print('options:')
    print('-h|--help')
    print('-t|--top-directory')
    print('-S|--starting-gun-file')
    print('-o|--operation-count')
    print('-d|--duration')
    print('-f|--max-files')
    print('-s|--max-file-size-kb')
    print('-r|--max-record-size-kb')
    print('-+r|--fix-record-size-kb')
    print('-R|--max-random-reads')
    print('-W|--max-random-writes')
    print('-Y|--fsyncs')
    print('-y|--fdatasyncs')
    print('-T|--response-times')
    print('-b|--bandwidth')
    print('-l|--levels')
    print('-D|--dirs-per-level')
    print('-w|--workload-table')
    print('-i|--report-interval')
    print('-a|--abreviated-stats')
    print('-+D|--random-distribution')
    print('-+v|--mean-velocity')
    print('-+d|--gaussian-stddev')
    print('-+c|--create_stddevs-ahead')
    print('-p|--pause_file')
    sys.exit(NOTOK)

# command line parameter variables here


starting_gun_file = None
top_directory = '/tmp/foo'
opcount = 0
duration = 1
max_files = 20
max_file_size_kb = 10
max_record_size_kb = 1
fix_record_size_kb = 0
max_random_reads = 2
max_random_writes = 2
fdatasync_probability_pct = 10
fsync_probability_pct = 20
short_stats = False
levels = 2
dirs_per_level = 3
rsptimes = False
bw = False
workload_table_filename = None
stats_report_interval = 0
# new parameters related to gaussian filename distribution
rand_distr_type = file_access_dist.UNIFORM
rand_distr_type_str = 'uniform'
mean_index_velocity = 0.0  # default is a fixed mean for the distribution
gaussian_stddev = 1000.0  # just a guess, means most of accesses within 1000 files?
# just a guess, most files will be created before they are read
create_stddevs_ahead = 3.0
drift_time = -1
pause_file = '/var/tmp/pause'


def parseopts():
    global top_directory, starting_gun_file, opcount, max_files, max_file_size_kb, duration, short_stats
    global max_record_size_kb, fix_record_size_kb, max_random_reads, max_random_writes, rsptimes, bw
    global fsync_probability_pct, fdatasync_probability_pct, workload_table_filename
    global stats_report_interval, levels, dirs_per_level
    global rand_distr_type, rand_distr_type_str, mean_index_velocity, gaussian_stddev, create_stddevs_ahead
    if len(sys.argv) % 2 != 1:
        usage('all options must have a value')
    try:
        ix = 1
        while ix < len(sys.argv):
            nm = sys.argv[ix]
            val = sys.argv[ix+1]
            ix += 2
            if nm == '--help' or nm == '-h':
                usage()
            elif nm == '--starting-gun-file' or nm == '-S':
                starting_gun_file = os.path.join(top_directory, val)
            elif nm == '--top-directory' or nm == '-t':
                top_directory = val
            elif nm == '--workload-table' or nm == '-w':
                workload_table_filename = val
            elif nm == '--operation-count' or nm == '-o':
                opcount = int(val)
            elif nm == '--duration' or nm == '-d':
                duration = int(val)
            elif nm == '--max-files' or nm == '-f':
                max_files = int(val)
            elif nm == '--max-file-size-kb' or nm == '-s':
                max_file_size_kb = int(val)
            elif nm == '--max-record-size-kb' or nm == '-r':
                max_record_size_kb = int(val)
            elif nm == '--fix-record-size-kb' or nm == '-+r':
                fix_record_size_kb = int(val)
            elif nm == '--max-random-reads' or nm == '-R':
                max_random_reads = int(val)
            elif nm == '--max-random-writes' or nm == '-W':
                max_random_writes = int(val)
            elif nm == '--fdatasync_pct' or nm == '-y':
                fdatasync_probability_pct = int(val)
            elif nm == '--fsync_pct' or nm == '-Y':
                fsync_probability_pct = int(val)
            elif nm == '--levels' or nm == '-l':
                levels = int(val)
            elif nm == '--dirs-per-level' or nm == '-D':
                dirs_per_level = int(val)
            elif nm == '--short-stats' or nm == '-a':
                short_stats = True
            elif nm == '--report-interval' or nm == '-i':
                stats_report_interval = int(val)
            elif nm == '--response-times' or nm == '-T':
                v = val.lower()
                rsptimes = (v == 'true' or v == 'yes' or v == 'on')
            elif nm == '--bandwidth' or nm == '-b':
                v = val.lower()
                bw = (v == 'true' or v == 'yes' or v == 'on')
            elif nm == '--random-distribution' or nm == '-+D':
                v = val.lower()
                if v == 'uniform':
                    rand_distr_type = file_access_dist.UNIFORM
                elif v == 'gaussian':
                    rand_distr_type = file_access_dist.GAUSSIAN
                else:
                    usage('random distribution must be "uniform" or "gaussian"')
                rand_distr_type_str = v
            elif nm == '--mean-velocity' or nm == '-+v':
                mean_index_velocity = float(val)
            elif nm == '--gaussian-stddev' or nm == '-+d':
                gaussian_stddev = float(val)
            elif nm == '--create_stddevs-ahead' or nm == '-+c':
                create_stddevs_ahead = float(val)
            elif nm == '--pause_file' or nm == '-p':
                pause_file = val
            else:
                usage('syntax error for option %s value %s' % (nm, val))
    except Exception as e:
        usage(str(e))
    print('')
    print((
        '%20s = top directory\n'
        '%20s = starting gun file\n'
        '%11s%9d = operation count\n'
        '%11s%9d = duration\n'
        '%11s%9d = maximum files\n'
        '%11s%9d = maximum file size (KB)\n'
        '%11s%9d = maximum record size (KB)\n'
        '%11s%9d = fix record size (KB)\n'
        '%11s%9d = maximum random reads\n'
        '%11s%9d = maximum random writes\n'
        '%11s%9d = fdatasync percentage\n'
        '%11s%9d = fsync percentage\n'
        '%11s%9d = directory levels\n'
        '%11s%9d = directories per level\n'
        '%20s = filename random distribution\n'
        '%11s%9.1f = mean index velocity\n'
        '%11s%9.1f = gaussian stddev\n'
        '%11s%9.1f = create stddevs ahead\n'
        '%20s = save response times\n'
        '%20s = save bandwidth\n'
        % (top_directory, str(starting_gun_file), '', opcount, '', duration, '', max_files, '', max_file_size_kb,
           '', max_record_size_kb, '', fix_record_size_kb, '', max_random_reads, '', max_random_writes,
           '', fdatasync_probability_pct, '', fsync_probability_pct,
           '', levels, '', dirs_per_level,
           rand_distr_type_str, '', mean_index_velocity, '', gaussian_stddev, '', create_stddevs_ahead,
           str(rsptimes), str(bw))))
    if workload_table_filename != None:
        print('%20s = workload table filename' % workload_table_filename)
    if stats_report_interval > 0:
        print('%11s%9d = statistics report intervalpercentage' %
              ('', stats_report_interval))
    if (duration == 1):
        print('do "python fs-drift.py --help" for list of command line parameters')
    sys.stdout.flush()


if __name__ == "__main__":
    parseopts()
