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
launcher=/fs-drift/launch_daemon.py
ls -l $launcher
echo "topdir: $topdir"
echo "container_id: $launch_id"
ls -l $topdir
# for RHEL, Python v2 is packaged in "python" package, for Fedora it's "python2"
(rpm -q python2 || rpm -q python || rpm -q python3) 2>/tmp/rpm.log
if [ -x /usr/bin/python ] ; then 
    export PYTHONPROG=/usr/bin/python
else
    export PYTHONPROG=/usr/bin/python3
fi
VERBOSE=1 $PYTHONPROG $launcher --top ${topdir} --as-host ${launch_id}
