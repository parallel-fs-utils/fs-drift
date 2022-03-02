# opts.py - module to parse command line options and output what parameters will be used for this run

import os
import os.path
import json
import sys
import yaml
from argparse import ArgumentParser

# fs-drift module dependencies

from common import OK, NOTOK, FsDriftException, FileAccessDistr, USEC_PER_SEC, BYTES_PER_KiB
from common import FileAccessDistr2str
from parser_data_types import boolean, positive_integer, non_negative_integer, bitmask, positive_integer_or_None
from parser_data_types import positive_float, non_negative_float, positive_percentage
from parser_data_types import host_set, file_access_distrib, size_or_range
from parser_data_types import FsDriftParseException, TypeExc


def getenv_or_default(var_name, var_default):
    v = os.getenv(var_name)
    if v == None:
        v = var_default
    return v

# command line parameter variables here

class FsDriftOpts:
    def derive_paths(self):
        self.starting_gun_path     = os.path.join(self.network_shared_path,'starting-gun.tmp')
        self.stop_file_path        = os.path.join(self.network_shared_path,'stop-file.tmp')
        self.param_pickle_path     = os.path.join(self.network_shared_path,'params.pickle')
        self.rsptime_path          = os.path.join(self.network_shared_path,'host-%s_thrd-%s_rsptimes.csv')
        self.abort_path            = os.path.join(self.network_shared_path,'abort.tmp')
        self.pause_path            = os.path.join(self.network_shared_path,'pause.tmp')
        self.checkerflag_path      = os.path.join(self.network_shared_path,'checkered_flag.tmp')

    def __init__(self):
        self.input_yaml = None
        self.output_json_path = None  # filled in later
        self.host_set = [] # default is local test
        self.top_directory = '/tmp/foo'
        self.threads = 2 #  number of subprocesses per host
        self.is_slave = False
        self.duration = 1
        self.max_files = 200
        self.max_file_size_kb = 10
        self.max_record_size_kb = None
        self.record_size = 4096
        self.fdatasync_probability_pct = 10
        self.fsync_probability_pct = 20
        self.levels = 2
        self.subdirs_per_dir = 3
        self.rsptimes = False
        self.workload_table_csv_path = None
        self.stats_report_interval = max(self.duration // 60, 5)
        self.pause_between_ops = 100
        self.pause_secs = self.pause_between_ops / float(USEC_PER_SEC)
        self.incompressible = False
        self.compress_ratio = 0.0
        self.dedupe_pct = 0
        self.directIO = False
        self.rawdevice = None
        # new parameters related to gaussian filename distribution
        self.random_distribution = FileAccessDistr.uniform
        self.mean_index_velocity = 1.0  # default is a fixed mean for the distribution
        # just a guess, means most of accesses are limited to 1% of total files 
        # so more cache-friendly
        self.gaussian_stddev = self.max_files * 0.01
        if self.max_files < 1000:
            self.gaussian_stddev = self.max_files * 0.1
        # just a guess, most files will be created before they are read
        self.create_stddevs_ahead = 3.0
        self.drift_time = -1
        self.mount_command = None
        self.fullness_limit_pct = 85
        # not settable
        self.is_slave = False
        self.as_host = None  # filled in by worker host
        self.verbosity = 0
        self.tolerate_stale_fh = False
        self.launch_as_daemon = False
        self.python_prog = getenv_or_default('PYTHONPROG', '/usr/bin/python3')
        self.fsd_remote_dir = getenv_or_default('FSD_REMOTE_DIR', '/usr/local/bin')

    def kvtuplelist(self):
        return [
            ('input YAML', self.input_yaml),
            ('top directory', self.top_directory),
            ('JSON output file', self.output_json_path),
            ('save response times?', self.rsptimes),
            ('stats report interval', self.stats_report_interval),
            ('workload table csv path', self.workload_table_csv_path),
            ('host set', ','.join(self.host_set)),
            ('threads', self.threads),
            ('test duration', self.duration),
            ('maximum file count', self.max_files),
            ('maximum file size (KiB)', self.max_file_size_kb),
            ('maximum record size (KiB)', self.max_record_size_kb),
            ('record size ', self.record_size),
            ('fsync probability pct', self.fsync_probability_pct),
            ('fdatasync probability pct', self.fdatasync_probability_pct),
            ('directory levels', self.levels),
            ('subdirectories per directory', self.subdirs_per_dir),
            ('incompressible data', self.incompressible),
            ('compression ratio', self.compress_ratio),
            ('deduplication percentage', self.dedupe_pct),            
            ('use direct IO', self.directIO),            
            ('use this device for raw IO', self.rawdevice),                      
            ('pause between ops (usec)', self.pause_between_ops),
            ('distribution', FileAccessDistr2str(self.random_distribution)),
            ('mean index velocity', self.mean_index_velocity),
            ('gaussian std. dev.', self.gaussian_stddev),
            ('create stddevs ahead', self.create_stddevs_ahead),
            ('mount command', self.mount_command),
            ('verbosity', self.verbosity),
            ('pause path', self.pause_path),
            ('abort path', self.abort_path),
            ('tolerate stale file handles', self.tolerate_stale_fh),
            ('fullness limit percent', self.fullness_limit_pct),
            ('launch using daemon', self.launch_as_daemon),
            ('python program', self.python_prog),
            ('fs-drift-remote.py directory', self.fsd_remote_dir),
            ]

    def __str__(self, use_newline=True, indentation='  '):
        kvlist = [ '%-40s = %s' % (k, str(v)) for (k, v) in self.kvtuplelist() ]
        if use_newline:
            return indentation + ('\n%s' % indentation).join(kvlist)
        else:
            return ' , '.join(kvlist)

    def to_json_obj(self):
        d = {}
        for (k, v) in self.kvtuplelist():
            d[k] = v
        return d

    def validate(self):

        if len(self.top_directory) < 6:
            raise FsDriftException(
                'top directory %s too short, may be system directory' % 
                self.top_directory)

        if not os.path.isdir(self.top_directory):
            raise FsDriftException(
                'top directory %s does not exist, so please create it' %
                self.top_directory)

        if self.workload_table_csv_path == None:
            self.workload_table_csv_path = os.path.join(self.top_directory, 
                                                        'example_workload_table.csv')
            workload_table = [
                    'read, 2',
                    'random_read, 1',
                    'random_write, 1',
                    'append, 4',
                    'delete, 0.1',
                    'hardlink, 0.01',
                    'softlink, 0.02',
                    'truncate, 0.05',
                    'rename, 1',
                    'readdir, 0.1',
                    'create, 4',
                    'write, 2']
            with open(self.workload_table_csv_path, 'w') as w_f:
                w_f.write( '\n'.join(workload_table))

def assure_block_alignment(size):
    if size < 4 * BYTES_PER_KiB:
        print('size too low for directIO, raising to 4KiB')    
        return 4 * BYTES_PER_KiB
    return 4 * BYTES_PER_KiB * round(size / (4 * BYTES_PER_KiB))
    
def resolve_size(size_input, directIO):
    if ':' not in size_input:
        size = size_unit_to_bytes(size_input)
        if directIO:
            return assure_block_alignment(size)
        return size
    else:
        low_bound, high_bound = size_input.split(':')
        low_bound = size_unit_to_bytes(low_bound)
        high_bound = size_unit_to_bytes(high_bound)
        if low_bound > high_bound:
            raise FsDriftException('low bound (left) should be larger than high bound (right), got %s' % size_input)
        if directIO:  
            return (assure_block_alignment(low_bound), assure_block_alignment(high_bound))
        return (low_bound, high_bound)

def parseopts(cli_params=sys.argv[1:]):
    o = FsDriftOpts()

    parser = ArgumentParser(description='parse fs-drift parameters')
    add = parser.add_argument
    add('--input-yaml', help='input YAML file containing parameters',
            default=None)
    add('--output-json', help='output file containing results in JSON format',
            default=None)
    add('--workload-table', help='.csv file containing workload mix',
            default=None)
    add('--duration', help='seconds to run test',
            type=positive_integer, 
            default=o.duration)
    add('--host-set', help='comma-delimited list of host names/ips',
            type=host_set,
            default=o.host_set)
    add('--top', help='directory containing all file accesses',
            default=o.top_directory)
    add('--threads', help='number of subprocesses per host',
            type=positive_integer,
            default=o.threads)
    add('--max-files', help='maximum number of files to access',
            type=positive_integer, 
            default=o.max_files)
    add('--max-file-size-kb', help='maximum file size in KiB',
            type=positive_integer, 
            default=o.max_file_size_kb)
    add('--pause-between-ops', help='delay between ops in microsec',
            type=non_negative_integer,
            default=o.pause_between_ops)
    add('--max-record-size-kb', help='maximum read/write size in KiB. Deprecated, use --record-size instead',
            type=positive_integer, 
            default=o.max_record_size_kb)
    add('--record-size', help='read/write record size. If no units specified, treated like B. Other units: k, m, g. For range, enter two values separated by ":". Eg. 4:64',
            type=size_or_range, 
            default=o.record_size)            
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
    add('--compress-ratio', help='desired compress ratio, e.g. 4.0 is compressibility of 75 percent, i.e. the compressed block occupies 25 percent of original space',
            type=positive_float,
            default=o.compress_ratio)
    add('--dedupe-pct', help='deduplication percentage, i.e. percentage of data blocks that will be deduplicable',
            type=positive_percentage,
            default=o.dedupe_pct)
    add('--directIO', help='if True then use directIO to open files/device',
            type=boolean,
            default=o.directIO)            
    add('--rawdevice', help='if set, use this device as a target for rawdevice testing (Warning: Data/File systems on this device will be corrupted)',
            default=o.rawdevice)                
    add('--random-distribution', help='either "uniform" or "gaussian"',
            type=file_access_distrib, 
            default=FileAccessDistr.uniform)
    add('--mean-velocity', help='rate at which mean advances through files',
            type=float, 
            default=o.mean_index_velocity)
    add('--gaussian-stddev', help='std. dev. of file number',
            type=float, 
            default=o.gaussian_stddev)
    add('--create-stddevs-ahead', help='file creation ahead of other opts by this many stddevs',
            type=float, 
            default=o.create_stddevs_ahead)
    add('--mount-command', help='command to mount the filesystem containing top directory',
            default=o.mount_command)
    add('--tolerate-stale-file-handles', help='if true, do not throw exception on ESTALE',
            type=boolean,
            default=o.tolerate_stale_fh)
    add('--fullness-limit-percent', help='stop adding to filesystem when it gets this full',
            type=positive_percentage,
            default=o.fullness_limit_pct)
    add('--verbosity', help='decimal or hexadecimal integer bitmask controlling debug logging',
            type=bitmask,
            default=o.verbosity)
    add('--launch-as-daemon', help='launch remote/containerized fs-drift without ssh',
            type=boolean,
            default=o.launch_as_daemon)

    # parse the command line and update opts
    args = parser.parse_args(cli_params)
    o.top_directory = args.top
    o.output_json_path = args.output_json
    o.rsptimes = args.response_times
    o.stats_report_interval = args.report_interval
    o.host_set = args.host_set
    o.threads = args.threads
    o.report_interval = args.report_interval
    o.workload_table_csv_path = args.workload_table
    o.duration = args.duration
    o.max_files = args.max_files
    o.max_file_size_kb = args.max_file_size_kb
    o.record_size = args.record_size
    if args.max_record_size_kb:
        o.record_size = (1, args.max_record_size_kb * BYTES_PER_KiB)
        o.max_record_size_kb = args.max_record_size_kb
    elif isinstance(o.record_size, tuple):
        o.max_record_size_kb = o.record_size[-1] // BYTES_PER_KiB
    else:
        o.max_record_size_kb = o.record_size // BYTES_PER_KiB
    o.fdatasync_probability_pct = args.fdatasync_pct
    o.fsync_probability_pct = args.fsync_pct
    o.levels = args.levels
    o.subdirs_per_dir = args.dirs_per_level
    o.incompressible = args.incompressible
    o.compress_ratio = args.compress_ratio
    o.dedupe_pct = args.dedupe_pct
    o.directIO = args.directIO
    if o.directIO:
        o.max_record_size_kb = assure_block_alignment(o.max_record_size_kb * BYTES_PER_KiB) // BYTES_PER_KiB
        if isinstance(o.record_size, tuple):
            o.record_size = (assure_block_alignment(o.record_size[0]), assure_block_alignment(o.record_size[1]))
        else:
            o.record_size = assure_block_alignment(o.record_size)     
    o.rawdevice = args.rawdevice        
    o.pause_between_ops = args.pause_between_ops
    o.pause_secs = o.pause_between_ops / float(USEC_PER_SEC)
    o.response_times = args.response_times
    o.random_distribution = args.random_distribution
    o.mean_index_velocity = args.mean_velocity
    o.gaussian_stddev = args.gaussian_stddev
    o.create_stddevs_ahead = args.create_stddevs_ahead
    o.mount_command = args.mount_command
    o.tolerate_stale_fh = args.tolerate_stale_file_handles
    o.fullness_limit_pct = args.fullness_limit_percent
    o.launch_as_daemon = args.launch_as_daemon
    o.verbosity = args.verbosity
    if args.input_yaml:
        print('parsing input YAML file %s' % args.input_yaml)
        parse_yaml(o, args.input_yaml)

    # some fields derived from user inputs

    o.network_shared_path = os.path.join(o.top_directory, 'network-shared')
    o.derive_paths()

    #o.remote_pgm_dir = os.path.dirname(sys.argv[0])
    #if o.remote_pgm_dir == '.':
    #    o.remote_pgm_dir = os.getcwd()

    o.is_slave = sys.argv[0].endswith('fs-drift-remote.py')
 
    return o


# module to parse YAML input file containing fs-drift parameters
# YAML parameter names are identical to CLI parameter names
#  except that the leading "--" is removed and single '-' characters
# must be changed to underscore '_' characters
# modifies test_params object with contents of YAML file

def parse_yaml(options, input_yaml_file):
    with open(input_yaml_file, 'r') as f:
        try:
            y = yaml.safe_load(f)
            if y == None:
                y = {}
        except yaml.YAMLError as e:
            emsg = "YAML parse error: " + str(e)
            raise FsDriftParseException(emsg)
    
    try:
        for k in y.keys():
            v = y[k]
            if k == 'input_yaml':
                raise FsDriftParseException('cannot specify YAML input file from within itself!')
            elif k == 'top':
                options.top_directory = v
                options.network_shared_path = os.path.join(v, 'network-shared')
                options.derive_paths()
            elif k == 'output_json':
                options.output_json = v
            elif k == 'workload_table':
                options.workload_table = v
            elif k == 'duration':
                options.duration = positive_integer(v)
            elif k == 'host_set':
                options.host_set = host_set(v)
            elif k == 'threads':
                options.threads = positive_integer(v)
            elif k == 'max_files':
                options.max_files = positive_integer(v)
            elif k == 'max_file_size_kb':
                options.max_file_size_kb = positive_integer(v)
            elif k == 'pause_between_ops':
                options.pause_between_ops = non_negative_integer(v)
            elif k == 'max_record_size_kb':
                options.max_record_size_kb = positive_integer_or_None(v)
            elif k == 'record_size':
                options.record_size = size_or_range(v)
            elif k == 'fdatasync_pct':
                options.fdatasync_probability_pct = non_negative_integer(v)
            elif k == 'fsync_pct':
                options.fsync_probability_pct = non_negative_integer(v)
            elif k == 'levels':
                options.levels = positive_integer(v)
            elif k == 'dirs_per_level':
                options.dirs_per_level = positive_integer(v)
            elif k == 'report_interval':
                options.stats_report_interval = positive_integer(v)
            elif k == 'response_times':
                options.rsptimes = boolean(v)
            elif k == 'incompressible':
                options.incompressible = boolean(v)
            elif k == 'compress-ratio':
                options.compress_ratio = float(v)                
            elif k == 'dedupe-pct':
                options.dedupe_pct = positive_percentage(v)
            elif k == 'directIO':
                options.directIO = boolean(v)
            elif k == 'rawdevice':
                options.rawdevice = v                    
            elif k == 'random_distribution':
                options.random_distribution = file_access_distrib(v)
            elif k == 'mean_velocity':
                options.mean_velocity = float(v)
            elif k == 'gaussian_stddev':
                options.gaussian_stddev = float(v)
            elif k == 'create_stddevs_ahead':
                options.create_stddevs_ahead = float(v)
            elif k == 'tolerate_stale_file_handles':
                options.tolerate_stale_fh = boolean(v)
            elif k == 'fullness_limit_percent':
                options.fullness_limit_pct = positive_percentage(v)
            elif k == 'verbosity':
                options.verbosity = bitmask(v)
            elif k == 'launch_as_daemon':
                options.launch_as_daemon = boolean(v)
            else:
                raise FsDriftParseException('unrecognized parameter name %s' % k)
        if options.max_record_size_kb:
            options.record_size = (1, options.max_record_size_kb * BYTES_PER_KiB)
            options.max_record_size_kb = options.max_record_size_kb
        elif isinstance(options.record_size, tuple):
            options.max_record_size_kb = options.record_size[-1] // BYTES_PER_KiB
        else:
            options.max_record_size_kb = options.record_size // BYTES_PER_KiB
        if options.directIO:
            options.max_record_size_kb = assure_block_alignment(options.max_record_size_kb * BYTES_PER_KiB) // BYTES_PER_KiB
            if isinstance(options.record_size, tuple):
                options.record_size = (assure_block_alignment(options.record_size[0]), assure_block_alignment(options.record_size[1]))
            else:
                options.record_size = assure_block_alignment(options.record_size)
    except TypeExc as e:
        emsg = 'YAML parse error for key "%s" : %s' % (k, str(e))
        raise FsDriftParseException(emsg)



if __name__ == "__main__":

    # if user supplies command line parameters

    if len(sys.argv) > 2:
        # accept CLI and parse it without doing anything else
        options = parseopts()
        options.validate()
        print(options)
        print('json format:')
        print(json.dumps(options.to_json_obj(), indent=2, sort_keys=True))
        sys.exit(0)

    # otherwise run unit test

    from unit_test_module import get_unit_test_module
    unittest_module = get_unit_test_module()

    class YamlParseTest(unittest_module.TestCase):
        def setUp(self):
            self.params = FsDriftOpts()

        def test_parse_all(self):
            params = []
            params.extend(['--top', '/var/tmp'])
            params.extend(['--output-json', '/var/tmp/x.json'])
            params.extend(['--workload-table', '/var/tmp/x.csv'])
            params.extend(['--duration', '60'])
            params.extend(['--threads', '30'])
            params.extend(['--max-files', '10000'])
            params.extend(['--max-file-size-kb', '1000000'])
            params.extend(['--pause-between-ops', '100'])
            params.extend(['--max-record-size-kb', '4096'])
            params.extend(['--record-size', '4k'])            
            params.extend(['--fdatasync-pct', '2'])
            params.extend(['--fsync-pct', '3'])
            params.extend(['--levels', '4'])
            params.extend(['--dirs-per-level', '50'])
            params.extend(['--report-interval', '60'])
            params.extend(['--response-times', 'Y'])
            params.extend(['--incompressible', 'false'])
            params.extend(['--directIO', 'false'])      
            params.extend(['--rawdevice', 'none'])                   
            params.extend(['--random-distribution', 'gaussian'])
            params.extend(['--mean-velocity', '4.2'])
            params.extend(['--gaussian-stddev', '100.2'])
            params.extend(['--create-stddevs-ahead', '3.2'])
            params.extend(['--tolerate-stale-file-handles', 'y'])
            params.extend(['--fullness-limit-percent', '80'])
            params.extend(['--verbosity', '0xffffffff'])
            params.extend(['--launch-as-daemon', 'Y'])
            options = parseopts(cli_params=params)
            options.validate()
            print(options)
            print('json format:')
            print(json.dumps(options.to_json_obj(), indent=2, sort_keys=True))

        def test_parse_all_from_yaml(self):
            fn = '/tmp/sample_parse.yaml'
            with open(fn, 'w') as f:
                w = lambda s: f.write(s + '\n')
                w('top: /var/tmp')
                w('output_json: /var/tmp/x.json')
                w('workload_table: /var/tmp/x.csv')
                w('duration: 60')
                w('threads: 30')
                w('max_files: 10000')
                w('max_file_size_kb: 1000000')
                w('pause_between_ops: 100')
                w('max_record_size_kb: None')
                w('record_size: 4096')                
                w('fdatasync_pct: 2')
                w('fsync_pct: 3')
                w('levels: 4')
                w('dirs_per_level: 50')
                w('report_interval: 60')
                w('response_times: Y')
                w('incompressible: false')
                w('directIO: false')                
                w('random_distribution: gaussian')
                w('mean_velocity: 4.2')
                w('gaussian_stddev: 100.2')
                w('create_stddevs_ahead: 3.2')
                w('tolerate_stale_file_handles: y')
                w('fullness_limit_percent: 80')
                w('verbosity: 0xffffffff')
                w('launch_as_daemon: Y')

            p = self.params
            parse_yaml(p, fn)
            assert(p.top_directory == '/var/tmp')
            assert(p.network_shared_path == '/var/tmp/network-shared')
            assert(p.stop_file_path == '/var/tmp/network-shared/stop-file.tmp')
            assert(p.output_json == '/var/tmp/x.json')
            assert(p.workload_table == '/var/tmp/x.csv')
            assert(p.duration == 60)
            assert(p.threads == 30)
            assert(p.max_files == 10000)
            assert(p.max_file_size_kb == 1000000)
            assert(p.pause_between_ops == 100)
            assert(p.max_record_size_kb == 4)
            assert(p.record_size == 4096)
            assert(p.fdatasync_probability_pct == 2)
            assert(p.fsync_probability_pct == 3)
            assert(p.levels == 4)
            assert(p.dirs_per_level == 50)
            assert(p.stats_report_interval == 60)
            assert(p.rsptimes == True)
            assert(p.incompressible == False)
            assert(p.directIO == False)            
            assert(p.rawdevice == None)            
            assert(p.random_distribution == FileAccessDistr.gaussian)
            assert(p.mean_velocity == 4.2)
            assert(p.gaussian_stddev == 100.2)
            assert(p.create_stddevs_ahead == 3.2)
            assert(p.tolerate_stale_fh == True)
            assert(p.fullness_limit_pct == 80)
            assert(p.verbosity == 0xffffffff)
            assert(p.launch_as_daemon == True)

        def test_parse_negint(self):
            fn = '/tmp/sample_parse_negint.yaml'
            with open(fn, 'w') as f:
                f.write('max_files: -3\n')
            try:
                parse_yaml(self.params, fn)
            except FsDriftParseException as e:
                msg = str(e)
                if not msg.__contains__('greater than zero'):
                    raise e

        def test_parse_hostset(self):
            fn = '/tmp/sample_parse_hostset.yaml'
            with open(fn, 'w') as f:
                f.write('host_set: host-foo,host-bar\n')
            parse_yaml(self.params, fn)
            assert(self.params.host_set == [ 'host-foo', 'host-bar' ])

    unittest_module.main()
