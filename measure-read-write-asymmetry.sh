#!/bin/bash
PATH_TO_CHECK="./build"

if [ ! -e "./build" ]; then
	echo "The \"build\" directory does not exist. Please ensure a build/ directory is created"
	exit
fi

if [ ! -e "./build/load-gen" ]; then
	echo "The load-gen executable file does not exist in a right path. Please follow the instruction to compile the project and ensure load-gen is generated under the build/ directory"
	exit
fi


if [ ! -e "./build/emul" ]; then
	echo "The emul executable file does not exist in a right path. Please follow the instruction to compile the project and ensure emul is generated under the build/ directory"
	exit
fi

if [ -e "./test-tmp" ]; then
	echo "Please rename test-tmp dirctory if it exists."
	exit
fi
mkdir -p test-tmp
cd test-tmp/
echo "../build/load-gen"
../build/load-gen
echo "../build/emul --PJM-GHJ -B 32768 --NoJoinOutput | tee GHJ_output.txt"
../build/emul --PJM-GHJ -B 32768 --NoJoinOutput | tee GHJ_output.txt
echo "../build/emul --PJM-SMJ -B 32768 --NoJoinOutput | tee SMJ_output.txt"
../build/emul --PJM-SMJ -B 32768 --NoJoinOutput | tee SMJ_output.txt
echo "../build/emul --PJM-GHJ --NoSyncIO -B 32768 --NoJoinOutput | tee GHJ_no_sync_output.txt"
../build/emul --PJM-GHJ --NoSyncIO -B 32768 --NoJoinOutput | tee GHJ_no_sync_output.txt
echo "../build/emul --PJM-SMJ --NoSyncIO -B 32768 --NoJoinOutput | tee SMJ_no_sync_output.txt"
../build/emul --PJM-SMJ --NoSyncIO -B 32768 --NoJoinOutput | tee SMJ_no_sync_output.txt
read_latency=`grep "Read Latency" GHJ_output.txt | awk '{print $(NF-1)}'`
random_write_sync_latency=`grep "Write Latency" GHJ_output.txt | awk '{print $(NF-1)}'`
sequential_write_sync_latency=`grep "Write Latency" SMJ_output.txt | awk '{print $(NF-1)}'`
mu_sync=$(echo "scale=2; ${random_write_sync_latency} / ${read_latency}" | bc)
tau_sync=$(echo "scale=2; ${sequential_write_sync_latency} / ${read_latency}" | bc)
echo -e "Sync I/O:\t mu=${mu_sync}, tau=${tau_sync}"
read_latency=`grep "Read Latency" GHJ_no_sync_output.txt | awk '{print $(NF-1)}'`
random_write_no_sync_latency=`grep "Write Latency" GHJ_no_sync_output.txt | awk '{print $(NF-1)}'`
sequential_write_no_sync_latency=`grep "Write Latency" SMJ_no_sync_output.txt | awk '{print $(NF-1)}'`
mu_no_sync=$(echo "scale=2; ${random_write_no_sync_latency} / ${read_latency}" | bc)
tau_no_sync=$(echo "scale=2; ${sequential_write_no_sync_latency} / ${read_latency}" | bc)

echo -e "No-Sync I/O:\t mu=${mu_no_sync}, tau=${tau_no_sync}"
cd -
rm -rf test-temp/
