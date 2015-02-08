#!/bin/bash
# kill-fsstress.sh - script to kill off fsstress processes on this host

for p in `ps awux | grep python | grep fsstress | grep -v grep | awk '{ print $2 }'` ; do echo $p ; kill -INT $p ; done
