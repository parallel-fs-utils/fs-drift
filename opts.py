# opts.py - module to parse command line options and output what parameters will be used for this run

import os
import os.path
import sys
import common
from common import OK, NOTOK
import argparse
import parser_data_types
from parser_data_types import boolean, positive_integer, non_negative_integer
from parser_data_types import positive_float, non_negative_float, positive_percentage
from parser_data_types import host_set, file_access_distrib, directory_list

# command line parameter variables here

class FsDriftOpts:
    def __init__(self):
        self.top_directory = '/tmp/foo'
        self.opcount = 0
        self.duration = 1
        self.max_files = 20
        self.max_file_size_kb = 10
        self.max_record_size_kb = 1
        self.max_random_reads = 2
        self.max_random_writes = 2
        self.fdatasync_probability_pct = 10
        self.fsync_probability_pct = 20
        self.levels = 2
        self.subdirs_per_dir = 3
        self.rsptimes = False
        self.workload_table_filename = None
        self.stats_report_interval = 0
        self.pause_between_ops = 100
        self.thread_fraction_done = 0.05
        self.incompressible = False
        # new parameters related to gaussian filename distribution
        self.rand_distr_type = common.FileAccessDistr.uniform
        self.mean_index_velocity = 0.0  # default is a fixed mean for the distribution
        self.gaussian_stddev = 1000.0  # just a guess, means most of accesses within 1000 files?
        # just a guess, most files will be created before they are read
        self.create_stddevs_ahead = 3.0
        self.drift_time = -1
        self.pause_file = '/var/tmp/pause'
        self.mount_command = None


def parseopts():
    o = FsDriftOpts()

    parser = argparse.ArgumentParser(description='parse fs-drift parameters')
    add = parser.add_argument
    add('--top', help='directory containing all file accesses',
            default=o.top_directory)
    add('--output-json', help='output file containing results in JSON format',
            default=None)
    add('--workload-table', help='CSV file containing workload mix',
            default=None)
    add('--operation-count', help='number of ops to perform',
            type=positive_integer, 
            default=o.opcount)
    add('--duration', help='seconds to run test',
            type=positive_integer, 
            default=o.duration)
    add('--max-files', help='maximum number of files to access',
            type=positive_integer, 
            default=o.max_files)
    add('--max-file-size-kb', help='maximum file size in KB',
            type=positive_integer, 
            default=o.max_file_size_kb)
    add('--pause-between-ops', help='delay between ops in microsec',
            type=non_negative_integer,
            default=200)
    add('--max-record-size-kb', help='maximum read/write size in KB',
            type=positive_integer, 
            default=o.max_record_size_kb)
    add('--max-random-reads', help='maximum consecutive random reads',
            type=positive_integer, 
            default=o.max_random_reads)
    add('--max-random-writes', help='maximum consecutive random writes',
            type=positive_integer, 
            default=o.max_random_writes)
    add('--fdatasync-pct', help='probability of fdatasync after write',
            type=positive_percentage, 
            default=o.fdatasync_probability_pct)
    add('--fsync-pct', help='probability of fsync after write',
            type=positive_percentage, 
            default=o.fsync_probability_pct)
    add('--levels', help='number of directory levels in tree',
            type=non_negative_integer, 
            default=o.levels)
    add('--dirs-per-level', help='number of subdirectories per directory',
            type=non_negative_integer,
            default=o.subdirs_per_dir)
    add('--report-interval', help='seconds between counter output',
            type=positive_integer,
            default=o.stats_report_interval)
    add('--response-times', help='if True then save response times to CSV file',
            type=boolean, 
            default=o.rsptimes)
    add('--incompressible', help='if True then write incompressible data',
            type=boolean,
            default=o.incompressible)
    add('--random-distribution', help='either "uniform" or "gaussian"',
            type=file_access_distrib, 
            default=common.FileAccessDistr.uniform)
    add('--mean-velocity', help='rate at which mean advances through files',
            type=float, 
            default=o.mean_index_velocity)
    add('--gaussian-stddev', help='std. dev. of file number',
            type=float, 
            default=o.gaussian_stddev)
    add('--create-stddevs-ahead', help='file creation ahead of other opts by this many stddevs',
            type=float, 
            default=o.create_stddevs_ahead)
    add('--thread-fraction-done', help='measurement done when this fraction of threads done',
            type=positive_float,
            default=o.thread_fraction_done)
    add('--pause-file', help='file access will be suspended when this file appears',
            default=o.pause_file)
    add('--mount-command', help='command to mount the filesystem containing top directory',
            default=o.mount_command)
    # parse the command line and update opts
    args = parser.parse_args()
    o.top_directory = args.top
    o.output_json = args.output_json
    o.pause_file = args.pause_file
    o.report_interval = args.report_interval
    o.workload_table_csv_path = args.workload_table
    o.duration = args.duration
    o.max_files = args.max_files
    o.max_file_size_kb = args.max_file_size_kb
    o.max_record_size_kb = args.max_record_size_kb
    o.max_random_reads = args.max_random_reads
    o.max_random_writes = args.max_random_writes
    o.fdatasync_probability_pct = args.fdatasync_pct
    o.fsync_probability_pct = args.fsync_pct
    o.levels = args.levels
    o.subdirs_per_dir = args.dirs_per_level
    o.incompressible = args.incompressible
    o.pause_between_ops = args.pause_between_ops
    o.thread_fraction_done = args.thread_fraction_done
    o.response_times = args.response_times
    o.random_distribution = args.random_distribution
    o.mean_index_velocity = args.mean_velocity
    o.gaussian_stddev = args.gaussian_stddev
    o.create_stddevs_ahead = args.create_stddevs_ahead
    o.mount_command = args.mount_command

    # some fields derived from user inputs

    o.network_shared_path = os.path.join(o.top_directory, 'network-shared')
    o.starting_gun_path = os.path.join(o.network_shared_path, 'starting-gun.tmp')
    o.stop_file_path = os.path.join(o.network_shared_path, 'stop-file.tmp')
    o.json_output_path = os.path.join(o.network_shared_path, 'results.json')
    o.param_pickle_path = os.path.join(o.network_shared_path, 'params.pickle')
    o.rsptime_path = os.path.join(o.network_shared_path, 'host-%s_thrd-%d_%%d_%%d_rspt.csv')

    # output params resulting from parse
    # even if user didn't specify them

    print('')
    print((
        '%20s = top directory\n'
        '%20s = JSON output file\n'
        '%20s = pause file\n'
        '%11s%9d = statistics report interval\n'
        '%20s = workload table\n'
        '%11s%9d = duration\n'
        '%11s%9d = operation count\n'
        '%11s%9d = maximum files\n'
        '%11s%9d = maximum file size (KB)\n'
        '%11s%9d = maximum record size (KB)\n'
        '%11s%9d = maximum random reads\n'
        '%11s%9d = maximum random writes\n'
        '%11s%9f = fdatasync percentage\n'
        '%11s%9f = fsync percentage\n'
        '%11s%9d = directory levels\n'
        '%11s%9d = directories per level\n'
        '%20s = incompressible\n'
        '%11s%9d = pause between ops\n'
        '%11s%9.7f = thread fraction done\n'
        '%20s = filename random distribution\n'
        '%11s%9.1f = mean index velocity\n'
        '%11s%9.1f = gaussian stddev\n'
        '%11s%9.1f = create stddevs ahead\n'
        '%20s = mount command\n'
        '%20s = save response times\n') % (
           o.top_directory, 
           o.output_json,
           o.pause_file, 
           '', o.stats_report_interval,
           str(o.workload_table_csv_path),
           '', o.duration, 
           '', o.opcount, 
           '', o.max_files, 
           '', o.max_file_size_kb, 
           '', o.max_record_size_kb, 
           '', o.max_random_reads, 
           '', o.max_random_writes, 
           '', o.fdatasync_probability_pct, 
           '', o.fsync_probability_pct, 
           '', o.levels, 
           '', o.subdirs_per_dir,
           str(o.incompressible),
           '', o.pause_between_ops,
           '', o.thread_fraction_done,
           common.FileAccessDistr2str(o.random_distribution), 
           '', o.mean_index_velocity, 
           '', o.gaussian_stddev, 
           '', o.create_stddevs_ahead, 
           o.mount_command,
           str(o.rsptimes)))
    sys.stdout.flush()
    return o

if __name__ == "__main__":
    options = parseopts()
