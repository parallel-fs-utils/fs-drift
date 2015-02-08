#!/bin/bash

t=$(mktemp -d --tmpdir=/mnt)

cd ~/src/fsstress

./fsstress.py -t ${t} -d 3500 -f 20000 -s 1 -w tier_workload.csv
