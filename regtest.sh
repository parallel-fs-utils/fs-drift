#!/bin/bash
# package dependencies: jq
# service dependencies: sshd
# setup dependencies: password-less ssh to localhost
# to run ssh-dependent tests, use environment variable test_ssh=1

OK=0  # process successful exit status
NOTOK=1 # process failure exit status

timestamp=`date +%Y-%m-%d-%H-%M`
logdir=/var/tmp/fs-drift-regtest-$timestamp
lognum=1
logf='not-here'
# if you want to use python v2,
# export PYTHON_PROG=/usr/bin/python
PY=${PYTHON_PROG:-/usr/bin/python3}

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
if [ -n "$test_ssh" ] ; then
	sudo systemctl start sshd || exit 1
	ssh -o PasswordAuthentication=no localhost pwd || exit 1
	chk "$PY ssh_thread.py"
fi
chk "$PY random_buffer.py"
chk "$PY worker_thread.py"
chk "$PY invoke_process.py"

chk "$PY opts.py -h > /tmp/o"
chk "grep 'option' /tmp/o"
mkdir -p /tmp/x.d
chk "$PY opts.py "
chkfail "$PY opts.py --top /x"


# test program that computes rates from counters

echo "testing rate computation"
rm -rf /tmp/fake-result
mkdir /tmp/fake-result
for hst in a b c ; do
  for thr in `seq -f "%02g" 1 2` ; do
    cat > /tmp/fake-result/counters.$thr.$hst.json <<EOF
[{
   "created": 2,
   "write_bytes": 1048576
},
{
   "created": 4,
   "write_bytes": 2097152
}]
EOF
  done
done
cat > /tmp/fake-result/result.json <<EOF
{
    "parameters": {
        "stats report interval": 5
    }
}
EOF
chk "params_json_fn=/tmp/fake-result/result.json ./compute-rates.py /tmp/fake-result"
chk "jq .[].created /tmp/fake-result/cluster-rates.json | grep '^2.40'"
chk "jq .[].write_MBps /tmp/fake-result/cluster-rates.json | grep '^1.20'"

chk "./fs-drift.py -h"
grep -iq 'usage: fs-drift.py' $logf || logf_fail
chkfail "./fs-drift.py --zzz"
grep -iq 'usage:' $logf || logf_fail

chk "./fs-drift.py"
chk "./fs-drift.py --random-distribution gaussian"

#Check directIO
chk "./fs-drift.py --duration 5 --record-size 4k --max-file-size-kb 32 --directIO True"
#Check if auto-alignment works even with bad values
chk "./fs-drift.py --duration 10 --record-size 1k --max-file-size-kb 1 --directIO True"

#Normal fs-drift usage (except the duration)
rm -rf /var/tmp/mydir
mkdir /var/tmp/mydir
chk "./fs-drift.py --top /var/tmp/mydir --duration 10 --response-times True --save-bw True --record-size 4k --max-file-size-kb 4096 --threads 8 --max-files 10 --report-interval 1 --random-distribution gaussian --mean-velocity 10.0 --directIO True --output-json /tmp/fs-drift-result.json"
export params_json_fn=/tmp/fs-drift-result.json
chk "./compute-rates.py /var/tmp/mydir/network-shared"

# test multi-host feature

if [ -n "$test_ssh" ] ; then
	rm -rf /var/tmp/mydir2
	mkdir /var/tmp/mydir2
	chk "./fs-drift.py --host-set localhost --top /var/tmp/mydir2 --response-times True --save-bw True --output-json /tmp/fs-drift-result2.json"
fi
