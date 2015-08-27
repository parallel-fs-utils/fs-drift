# shell parameters for distributed fs-drift
# this script is input to run_threads.sh
# parameter values here are examples, edit as needed

drift_pgm="./fs-drift.py"
topdir=${fss_topdir:-/mnt/ramfs/drift}
logdir=${fss_logdir:-/tmp/drift-logs}
workload=my_workload.csv
threads=${fss_threads:-6}
files=30000 
size=64
recsz=32
duration=10
interval=5
levels=3
dirs_per_level=5
