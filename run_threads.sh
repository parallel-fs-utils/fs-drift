#!/bin/bash 
#
# run_threads.sh - run multiple processes with fs-drift.py 
# edit test parameters in multi_thread_config.sh
#

. ./multi_thread_config.sh

starting_gun=starting-gun.tmp
drift_opts="-t $topdir -S $starting_gun -d $duration -f $files -s $size -r $recsz -l $levels -i $interval "
if [ -n "$workload" ] ; then
  drift_opts="$drift_opts -w $workload "
fi
cd `dirname $drift_pgm`
rm -rf $logdir
mkdir -p $logdir
if [ -z "$threads" ] ; then
  echo "usage: ./run_threads.sh thread-count"
  exit 1
fi
if [ $threads -gt 0 ] ; then
  for n in `seq 1 $threads` ; do 
    eval "$drift_pgm $drift_opts > $logdir/`hostname`.thr$n.log 2>&1 &" 
    pids="$pids $!"
  done
fi
sleep 2
if [ -z "$fss_multihost_test" ] ; then
  touch $topdir/$starting_gun
fi
worst_status=0
for p in $pids ; do
  wait $p
  s=$?
  if [ $s != 0 ] ; then 
    worst_status=$s
    echo "pid $p status $s"
  fi
done
exit $worst_status
