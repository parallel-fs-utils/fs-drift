#!/bin/bash
OK=0  # process successful exit status
NOTOK=1 # process failure exit status

timestamp=`date +%Y-%m-%d-%H-%M`
logdir=/var/tmp/fs-drift-regtest-$timestamp
lognum=1
logf='not-here'
# if you want to use python v3,
# export PYTHON_PROG=python3
PY=${PYTHON_PROG:-/usr/bin/python}
sudo systemctl start sshd

# both of these scripts take a command string (in quotes) as param 1

gen_logf()
{
  logf=$logdir/$lognum.log
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

mkdir $logdir

# run unit tests first

chk "$PY fsop.py"
chk "$PY event.py"
chk "$PY ssh_thread.py"
chk "$PY random_buffer.py"
chk "$PY worker_thread.py"
chk "$PY invoke_process.py"
chk "$PY opts.py -h > /tmp/o"
chk "grep 'optional arguments' /tmp/o"
mkdir /tmp/x.d
chk "$PY opts.py --top /tmp/x.d"
chkfail "$PY ./opts.py --top /"

chk "./fs-drift.py"
chk "./fs-drift.py -h"
grep -iq 'usage: fs-drift.py' $logf || logf_fail
chkfail "./fs-drift.py --zzz"
grep -iq 'all options must have a value' $logf || logf_fail


