#!/bin/bash
# kill-fs-drift.sh - script to kill off fs-drift processes on this host

for p in `ps awux | grep python | grep fs-drift | grep -v grep | awk '{ print $2 }'` ; do echo $p ; kill -INT $p ; done
