#!/bin/bash
OK=0  # process successful exit status
NOTOK=1 # process failure exit status

timestamp=`date +%Y-%m-%d-%H-%M`
logdir=/var/tmp/fs-drift-regtest-$timestamp
lognum=1
logf='not-here'
# if you want to use python v2,
# export PYTHON_PROG=/usr/bin/python
PY=${PYTHON_PROG:-/usr/bin/python3}
sudo systemctl start sshd

# both of these scripts take a command string (in quotes) as param 1

gen_logf()
{
  lognumstr=`seq -f '%02g' $lognum $lognum`
  logf=$logdir/$lognumstr.log
  (( lognum = $lognum + 1 ))
} 

chk()
{
  gen_logf
  echo "in $logf : $1"
  eval "$1" > $logf 2>&1
  if [ $? != $OK ] ; then echo ERROR ; exit $NOTOK ; fi
}

chkfail()
{
  gen_logf
  echo "in $logf : $1"
  eval "$1" > $logf 2>&1
  if [ $? == $OK ] ; then echo ERROR ; exit $NOTOK ; fi
}

logf_fail()
{
  echo "ERROR: expected result not found in logfile $logf"
  exit $NOTOK
}

rm -rf $logdir
mkdir $logdir
echo "log directory is $logdir"

# run unit tests first

chk "$PY fsop.py"
chk "$PY event.py"
chk "$PY ssh_thread.py"
chk "$PY random_buffer.py"
chk "$PY worker_thread.py"
chk "$PY invoke_process.py"
chk "$PY opts.py -h > /tmp/o"
chk "grep 'option' /tmp/o"
mkdir -p /tmp/x.d
chk "$PY opts.py "
chkfail "$PY opts.py --top /x"
chk "./fs-drift.py"
chk "./fs-drift.py -h"
grep -iq 'usage: fs-drift.py' $logf || logf_fail
chkfail "./fs-drift.py --zzz"
grep -iq 'usage:' $logf || logf_fail

chk "./fs-drift.py --random-distribution gaussian"

