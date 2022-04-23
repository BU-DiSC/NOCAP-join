#!/bin/bash
PJM_LIST=("GHJ" "MatrixDP" "ApprMatrixDP" "ApprMatrixDP --RoundedHash")
NAME_LIST=("GHJ" "MatrixDP" "ApprMatrixDP" "ApprMatrixDP--RoundedHash")
DIST="uni"
B=320
../build/load-gen
for index in "${!PJM_LIST[@]}"
do
	../build/emul --ClctPartStatsOnly --PJM-${PJM_LIST[$index]} -B ${B} --stats-path-output="part-stats-${NAME_LIST[$index]}-${DIST}-${B}.txt"
done



DIST="zipf"
../build/load-gen --JD 3
for index in "${!PJM_LIST[@]}"
do
	../build/emul --ClctPartStatsOnly --PJM-${PJM_LIST[$index]} -B ${B} --stats-path-output="part-stats-${NAME_LIST[$index]}-${DIST}-${B}.txt"
done



