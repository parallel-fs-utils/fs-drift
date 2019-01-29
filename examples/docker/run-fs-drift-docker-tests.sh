#!/bin/bash
# this script demonstrates how to run fs-drift in docker containers
# and drive load from all of those containers 
# the same ideas should work for Kubernetes (or OpenShift) pods

# parameter 1 is directory where fs-drift lives on the test driver host
# this is not the same as where it lives in the container (/)

fsdrift_dir=$1

# parameter 2 is the docker image name 

image=$2

if [ "$2" = "" ] ; then
  echo 'usage: run-fs-drift-docker-test.sh fs-drift-dir image'
  exit 1
fi
export PATH=$PATH:$fsdrift_dir
topdir=/var/tmp/fsddocker
# container counts should be powers of 2
min_containers=1
#max_containers=4
max_containers=4
total_files=100000
duration=20

# you should not have to edit below this line

d="sudo docker "

function shutdown_containers()
{
  echo "cleaning up any old containers..."
  $d ps
  c=$1
  for n in `seq 1 $c` ; do 
    $d stop --time=1 fsd-svr$n
    $d logs fsd-svr$n
    $d rm fsd-svr$n
  done
}

fsdrift_cli=fs-drift.py

# create filesystem directory shared by containers

sudo rm -rf $topdir
mkdir -pv $topdir

# create top-level log directory

timestamp=`date "+%m-%d-%H-%M-%S" `
logdir=`pwd`/logs/$timestamp
mkdir -pv $logdir
rm -f logs/latest.l
ln -sv $logdir logs/latest.l
cp $0 $logdir/

if [ -z "$KEEP_OLD_CONTAINERS" ] ; then
  shutdown_containers $max_containers 

  echo "starting up new set of containers"
  rm -fv $logdir/fsd-servers.list
  for n in `seq 1 $max_containers` ; do
    cmd="$d run -v $topdir:$topdir:z -e topdir=$topdir -e launch_id="container_$n" -d --name fsd-svr$n $image"
    echo "$cmd"
    $cmd
    echo "container_$n" >> $logdir/smf-servers.list
  done
fi

sleep 1

count=$min_containers
env | grep ETA_
while [ $count -le $max_containers ] ; do 
  (( files_per_thread = $total_files / $count ))
  rundir=$logdir/count.$count.op.$op
  mkdir -pv $rundir
  head -n $count $logdir/smf-servers.list > $rundir/smf-servers.list
  cmd="$fsdrift_cli --top $topdir --output-json json.log "
  cmd="$cmd --launch-as-daemon Y --host-set=$rundir/smf-servers.list"
  cmd="$cmd --threads 1 --max-files $files_per_thread --max-file-size-kb 4 --duration $duration"
  echo "$cmd"
  (cd $rundir ; $cmd 2>&1 | tee run.log )
  if [ $? != 0 ] ; then break ; fi
  (( count = $count * 2 ))
done

if [ -z "$LEAVE_RUNNING" ] ; then
  shutdown_containers $max_containers
fi
