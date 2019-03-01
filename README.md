# fs-drift
mixed-workload filesystem aging test

You must provide python's numpy module on every host where fs-drift runs.  For example:

    ansible -m shell -a 'yum install -y python-numpy' all

For a list of options usable with this script, "./fs-drift.py -h" .

To run it: ./fs-drift.py

This of course is a very small run, it just gives you a rough idea of the input parameters and the results you can generate.

To run it from multiple hosts, you must ensure that fs-drift-remote.py or a softlink to it is in the PATH environment variable.  For example:

    ansible -m shell -a 'ln -svf ~/fs-drift/fs-drift-remote.py /usr/local/bin/' clients

fs-drift is a program that attempts to stress a filesystem in various ways over a long period of time, in the following ways:
- file and record sizes are completely random
- random sequences of reads and writes
- random sequences of creates and deletes

Over a long enough period of time it is hoped that this behavior will induce filesystem aging and stress comparable to
that experienced by filesystems that have run for months or years.

The workload mix file is a .csv-format file , with each record containing an operation type and share (relative fraction) of operations of that type.  Operations are selected randomly in such a way that over a long enough period of time, the share determines the fraction of operations of that type.

The program outputs counters on a regular (selectable) interval that describe its behavior - for example, one counter is the number of times that a file could not be created because it already existed.  These counters can be converted into .csv format after the test completes using the parse_stress_log.py program, and then the counters can be aggregated, graphed, etc.
A basic design principle is use of randomness - we do not try to directly control the mix of reads and writes - instead we randomly generate a mix of requests, and over time the system reaches an equilibrium state.  For example, filenames are generated at random (default is uniform distribution).  When the test starts on an empty directory tree, creates succeed and reads fail with "file not found", so the directory tree fills up with files.  As the maximum file count is approached, we start to see create failures of the form "file already exists", or if the test is big enough, create/append failures such as "no space".  On the other hand, as the directory tree fills up, reads, deletes and other operation types are more likely to succeed.  At some point, we reach an equilibrium where total space used stabilizes, create/read mix stabilizes, and then we can run the test forever in this state.  So certain types of filesystem error returns are expected and counted, but any other filesystem errors result in an exception being logged.

In order for python GIL (Global Interpreter Lock) to not be a bottleneck, we create multiple subprocesses to be workload
generators, using the python multiprocessing module.

Synchronization between threads (subprocesses) is achieved with a directory shared by all threads and hosts, the network_shared/
subdirectory within the top directory.  We store the test parameter python object as a "pickle" file in network_shared/, and all subprocesses on all hosts read
it from there.  Each host announces its readiness by creating a host_ready file there, and when all hosts are ready, the
fs-drift.py master process broadcasts the start of the test (referred to as the "starting gun") by creating a file
there.  When a subprocess finishes, it posts its results as a python pickle in there.

Log files are kept in /var/tmp/fsd\*.log . 

To pause a test, just touch network_shared/pause.tmp
To resume a paused test, just remove network_shared/pause.tmp

## parameters

Every input parameter name is preceded by "--".  We don't bother with short-form parameter names.

Inputs:

--help

gives you parameter names and brief reminder of what they do.

--top-directory

Default: /tmp/foo -- where fs-drift puts all its files.  Note: YOU must create this directory.  Since the design center for fs-drift is distributed filesystems, we don't support multiple top-level directories (yet).  fs-drift leaves any existing files or subdirectories in place, so that it can be easily restarted - this is important for a longevity test.  However, the network_shared/ subdirectory inside the top directory is recreated each time it is run.

--output-json

Default: None -- if specified, this is the path where JSON counters are output to.

--response-times

Default: False -- If true, save response time data for each thread to a .csv file in the network shared directory. Each record in this file contains 2 comma-separated floating-point values.  The first value is number of seconds after start of the test. The second value is number of seconds the operation lasted. Response times for different operations are separated. 

--workload-table

Default: None (fs-drift will generate one) -- if specified, fs-drift will read the desired workload mix from this file.
Each record in the file contains 2 comma-separated values, the operation type and a "share" number (floating-pt) that determines the
fraction of operations of this type.  To compute the fraction, fs-drift adds all the shares together and then divides
each share by the total to get the fraction.  By doing it this way, the user does not have to calculate
percentages/fractions and make them all add up to 100/1.

--duration

Default: 1 -- specify test duration in seconds.

--host-set

Default: None -- specify set of remote hosts to generate workload, either in CSV-list form or as a pathname of a file that contains a list of hosts (1 per line).fs-drift will start up fs-drift-remote.py processes
on each of these hosts with same input parameters - you must have a filesystem shared by all of these host and the "initiator" host where you run fs-drift.py, and you must have password-less ssh access from the initiator host to all of the hosts in this parameter.  If no host set is specified, subprocesses will be created directly from your fs-drift.py process and run locally.

--threads

Default: 2 -- how many subprocesses/host will be generating workload.  We use subprocesses instead of python threads so that
we can utilize more than 1 CPU core per host.  But all subprocesses are just running same workload generator loop.

--max-files

Default: 20 -- Set a limit on the maximum number of files that can be accessed by fs-drift.  This allows us to run tests where we use a small fraction of the filesystem's space.  To fill up a filesystem, just specify a --max-files and a mean file size such that the product is much greater than the filesystem's space.

--max-file-size-kb

Default: 10 - Set a limit on maximum file size in KB.  File size is randomly generated and can be much less than this.

--max-record-size-kb

Default: 1 - Set a limit on maximum record size in KB.  Record (I/O transfer) size is randomly generated and can be much less than this.

--max-random-reads

Default: 2 -- Set a limit on how many random reads in a row are done to a file per random read op.

-max-random-writes

Default: 2 -- Set a limit on how many random writes in a row can be done to a file per random write op.

--fsync-pct

Default: 20 -- If true, allows fsync() call to be done every so often when files are written. Value is probability in percent.

--fdatasync-pct

Default: 10 -- If true, allows fdatasync() call to be done every so often when files are written. Value is probability in percent.

--levels

Default: 2 -- How many directory levels will be used.

--dirs-per-level

Default: 3 -- How many directories per level will be used.

--workload-table

Default: None - Provide a user-specified workload table controlling the mix of random operations.

--report-interval

Default: 0 -- Report counters over a user-specified interval.

--random-distribution

default: uniform -- filename access distribution is random uniform, but with this parameter set to "gaussian" you can create a non-uniform distribution of file access.  This is useful for caching and cache tiering systems.

--mean-velocity

default: 0.0 -- By default, a non-uniform random filename distribution is stationary over time, but with this parameter you can make the mean "move" at a specified velocity (i.e. the file number mean will shift by this much for every operation, modulo the maximum number of files.

--gaussian-stddev

Default: 1000.0 -- For gaussian filename distribution, this parameter controls with width of the bell curve.  As you increase this parameter past the cache space in your caching layer, the probability of a cache hit will go down.

--create_stddevs-ahead

Default: 3.0 -- This parameter is for cache tiering testing.  It allows creates to "lead" all other operations, so that we can create a high probability that read files will be in the set of "hot files".  Otherwise, most read accesses with non-uniform filename distribution will result  in "file not found" errors.

--pause-between-ops

Default: 100 microseconds -- This parameter is there to prevent some threads from getting way ahead of other threads in
tests where there are a lot of threads running.  It may not prove to be important.

--mount-command

Default: None -- For workload mixes that include the "remount" operation type, fs-drift.py will occasionally remount the
filesystem.  This tests the ability of the filesystem to respond quickly and correctly to these requests while under load.  
The user must specify a full mount command to use this operation type.  You must specify the mountpoint directory as the
last parameter in the command.  This allows fs-drift to do the unmount operation that precedes the mount.

--fullness-limit-percent

Default: 85 - Because fs-drift depends on the filesystem under test to return results from the worker threads to the test driver host (where fs-drift.py was originally run), we don't want the filesystem to fill up so much that we can't create any files in it.   You can set this any valid percentage, the realistic limit may depend on which filesystem you are running on.

## future enhancements

- logging - is a bit chaotic, done differently in different places, too many log files,
should be simple and user-controllable while the test is running.

- make remounts work with multiple threads on a host

- allow mountpoint per process 
-- to simulate large client populations
-- to support block storage tests

- extend number of filesystem operations

- dynamic parameter adjustment - want to be able to change parameters while the test is running (to see how the
  filesystem responds to major expansion/contraction, for example, or to see how different workload mixes impact the
  filesystem without having to run a whole new test.

- compression control - want to be able to specify buffers with different levels of compressibility

- elastic search - import JSON results into Elastic Search so we can visualize results in Grafana
