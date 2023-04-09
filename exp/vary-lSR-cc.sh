#!/bin/bash
TRIES=2
threads=1
lSR_list=("0.2" "0.4" "0.6" "0.8" "1.0")
rSR_list=("1.0")
for lSR in ${lSR_list[@]}
do
	for rSR in ${rSR_list[@]}
	do
		python3 vary-buffer-cc-with-selection.py --tries ${TRIES} --lSR ${lSR} --rSR ${rSR} --threads ${threads} --lTS 1000000 --rTS 8000000 --JD 3 --OP lSR-${lSR}-rSR-${rSR}-threads-${threads}.txt
	done
done

lSR_list=("1.0")
rSR_list=("0.15" "0.3" "0.45" "0.6")

for lSR in ${lSR_list[@]}
do
	for rSR in ${rSR_list[@]}
	do
		python3 vary-buffer-cc-with-selection.py --tries ${TRIES} --lSR ${lSR} --rSR ${rSR} --threads ${threads} --lTS 1000000 --rTS 8000000 --JD 3 --OP lSR-${lSR}-rSR-${rSR}-threads-${threads}.txt
	done
done
