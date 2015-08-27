#!/bin/bash
#
# run_clients.sh - script to launch fs-drift.py processes on all clients listed in clients.list file
#
#
threads_per_client=$1
logdir=/var/tmp/drift-logs
topdir=${fss_topdir:-/mnt/ramfs/drift2}
starting_gun=starting-gun.tmp
counter_topdir=/var/tmp/drift_counters
any_client=`head -1 clients.list` 

rm -fv $topdir/$starting_gun
SSH="ssh -o StrictHostKeyChecking=no -x "
sleep 1
for c in `cat clients.list` ; do
  eval "$SSH $c cd `pwd` && eval fss_topdir=$topdir fss_multihost_test=Y fss_logdir=$logdir ./run_threads.sh &"
  sshpids="$sshpids $!"
  echo "client $c pid $p"
done
sleep 5
$SSH  $any_client touch $topdir/$starting_gun
for p in $sshpids ; do
  wait $p
  echo "proc $p status $?"
done
rm -rf $counter_topdir
mkdir $counter_topdir
for n in `cat clients.list` ; do 
  d=$counter_topdir/$n
  scp -rqB $n:$logdir $d
  threads=`ls $d | wc -l`
  for t in `seq 1 $threads` ; do 
    ./parse_drift_log.py < $d/$n.thr$t.log > $d/thr$t.csv
  done
done
