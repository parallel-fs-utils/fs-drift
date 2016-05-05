#!/bin/bash
#
# script to extract filename distribution over time from
# strace -f -e open -o /tmp/s.log

grep f00 | \
  tr ':(),"/|' '      ' | \
  awk '{ print $4, $9, $10 }' | \
  sed 's/f00000//' | \
  sed 's/ [0]*/ /' | \
  awk '{printf "%s, %s, %s\n", $1, $2, $3}'
