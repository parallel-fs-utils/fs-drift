# fs-drift
mixed-workload filesystem aging test

For a list of options usable with this script, "./fs-drift.py -h" .

To run it: ./fs-drift.py

This of course is a very small run, it just gives you a rough idea of the input parameters and the results you can generate.

fs-drift is a program that attempts to stress a filesystem in various ways over a long period of time, in the following ways:
- file and record sizes are completely random
- random sequences of reads and writes
- random sequences of creates and deletes
Over a long enough period of time it is hoped that this behavior will induce filesystem aging.

A basic design principle is use of randomness - we do not try to directly control the mix of reads and writes - instead we randomly generate a mix of requests, and over time the system reaches an equilibrium state.  For example, filenames are generated at random (default is uniform distribution).  When the test starts on an empty directory tree, creates succeed and reads fail with "file not found", so the directory tree fills up with files.  As the maximum file count is approached, we start to see create failures of the form "file already exists", or if the test is big enough, create/append failures such as "no space".  On the other hand, as the directory tree fills up, reads, deletes and other operation types are more likely to succeed.  At some point, we reach an equilibrium where total space used stabilizes, create/read mix stabilizes, and then we can run the test forever in this state.

The workload mix file is a .csv-format file , with each record containing an operation type and share (relative fraction) of operations of that type.  Operations are selected randomly in such a way that over a long enough period of time, the share determines the fraction of operations of that type.  

The program outputs counters on a regular (selectable) interval that describe its behavior - for example, one counter is the number of times that a file could not be created because it already existed.  These counters can be converted into .csv format after the test completes using the parse_stress_log.py program, and then the counters can be aggregated, graphed, etc.

Every input parameter has a long form and a short form in traditional Linux style.

Inputs:

-t|--top-directory

where fs-drift puts all its files.  Since the design center for fs-drift is distributed filesystems, we don't support multiple top-level directories.  However, you can run multiple instances of fs-drift with different top-level directories and aggregate the results yourself.(default '/tmp/foo')

-S|--starting-gun-file

When run in distributed mode, fs-drift processes wait until they see this file before the run actually starts.  This allows fs-drift to synchronize start and stop of testing across systems, crucial for result aggregation across processes.(default None)

-o|--operation-count

How many operations fs-drift should attempt.  Note that this includes operations that abort with an expected system call error, such as create of a file that already exists.(default 0)

-d|--duration

Generally it's best to specify test duration in seconds instead of operation count.(default 1)

-f|--max-files

Set a limit on the maximum number of files that can be accessed by fs-drift.  This allows us to run tests where we use a small fraction of the filesystem's space.  To fill up a filesystem, just specify a --max-files and a mean file size such that the product is much greater than the filesystem's space.(default 20)

-s|--max-file-size-kb

Set a limit on maximum file size in KB.  File size is randomly generated and can be much less than this.(default 10)

-r|--max-record-size-kb

Set a limit on maximum record size in KB.  Record (I/O transfer) size is randomly generated and can be much less than this.(default 1)

-+r|--fix-record-size-kb

Set a fix record size for random operations in KB. Record (I/O transfer) size will be this much for random operations. If set to 0, record size will be generated the same way as for other operations. (default 0)

-R|--max-random-reads

Set a limit on how many random reads in a row are done to a file per random read op.(default 2)

-W|--max-random-writes

Set a limit on how many random writes in a row can be done to a file per random write op.(default 2)

-Y|--fsyncs

If true, allows fsync() call to be done every so often when files are written. Value is probability in percent. (default 20)

-y|--fdatasyncs

If true, allows fdatasync() call to be done every so often when files are written. Value is probability in percent. (default 10)

-T|--response-times

If true, save response time data to a .csv file. First value is number of seconds after start of the test. Second value is number of seconds the operation lasted. Response times for different operations are separated. (default False)

-b|--bandwidth

If truem save bandwidth data to a csv file. First value is number of seconds after start of the thest. Second value is bandwidth[kB/s]. Recorded values are for sequential reads (read), random reads (randread), random writes (randwrite) and sequential writes (write). Sequential writes are agregated from append and create operations.

-l|--levels

How many directory levels will be used. (default 2)

-D|--dirs-per-level

How many directories per level will be used. (default 3)

-w|--workload-table

Provide a user-specified workload table controlling the mix of random operations.(default None)

-i|--report-interval

Report counters over a user-specified interval. (default 0)

-+D|--random-distribution

By default, filename access frequency is random uniform, but with this parameter set to "gaussian" you can create a non-uniform distribution of file access.  This is useful for caching and cache tiering systems. (default 'uniform')

-+v|--mean-velocity

By default, a non-uniform random filename distribution is stationary over time, but with this parameter you can make the mean "move" at a specified velocity (i.e. the file number mean will shift by this much for every operation, modulo the maximum number of files. (default 0.0)

-+d|--gaussian-stddev

For gaussian filename distribution, this parameter controls with width of the bell curve.  As you increase this parameter past the cache space in your caching layer, the probability of a cache hit will go down. (default 1000.0)

-+c|--create_stddevs-ahead

This parameter is for cache tiering testing.  It allows creates to "lead" all other operations, so that we can create a high probability that read files will be in the set of "hot files".  Otherwise, most read accesses with non-uniform filename distribution will result  in "file not found" errors. (default 3.0)

-p|--pause

Parameter allows to specify pause file. If this file exists, fs-drift won't perform any I/O operations. (default /var/tmp/pause)




