#!/bin/bash
OK=0  # process successful exit status
NOTOK=1 # process failure exit status

timestamp=`date +%Y-%m-%d-%H-%M`
logdir=/var/tmp/fs-drift-regtest-$timestamp
lognum=1
logf='not-here'

mkdir -pv $logdir

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

chk "./fs-drift.py"
chkfail "./fs-drift.py -h"
grep -iq 'usage: fs-drift.py' $logf || logf_fail
chkfail "./fs-drift.py -zzz"
grep -iq 'all options must have a value' $logf || logf_fail
