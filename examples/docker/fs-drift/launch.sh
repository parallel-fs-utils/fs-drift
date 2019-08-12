#!/bin/bash -x
# this script is run by a container which should be launched 
# something like this:
#
#  #  d run -v /var/tmp/fsddocker:/var/tmp/fsddocker:z \
#         -e topdir=/var/tmp/fsddocker \
#         -e launch_id=container_1 \
#          bengland/fs-drift:20190128
#
# specifically you have to pass 2 environment variables:
#   topdir - points to container-local directory
#   launch_id - what container name should be
#   the -v volume option just imports a directory from the
#   host with SELinux set up to allow this (:z suffix)
#
fs_drift_srcdir=/fs-drift

launcher=$fs_drift_srcdir/launch_daemon.py
ls -l $launcher || exit $NOTOK

echo "topdir: $topdir"
echo "container_id: $launch_id"

# for RHEL, Python v2 is packaged in "python" package, for Fedora it's "python2"
(rpm -q python2 || rpm -q python || rpm -q python3) 2>/tmp/rpm.log
# prefer python3 but we run either one
if [ -x /usr/bin/python3 ] ; then 
    export PYTHONPROG=/usr/bin/python3
else
    export PYTHONPROG=/usr/bin/python
fi

# if user wants to run fs-drift independently in these containers
# just pass CLI parameters to it

if [ -z "$launch_id" ] ; then
    $PYTHONPROG $fs_drift_srcdir/fs-drift.py $*   
else
    # set verbosity parameter to 0xffffffff for maximum debug info
    $PYTHONPROG $launcher --top ${topdir} --as-host ${launch_id}
fi
