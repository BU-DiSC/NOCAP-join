#!/bin/bash
TRIES=10
threads=15
lSR_list=("0.2" "0.4" "0.6" "0.8" "1.0")
for lSR in ${lSR_list[@]}
do
	python3 vary-buffer-cc-with-selection.py --tries ${TRIES} --lSR ${lSR} --rSR 0.8 --threads ${threads} --lTS 1000000 --rTS 8000000 --JD 3 --OP lSR-${lSR}-rSR-0.8-threads-${threads}-no-sync-io.txt
done
