#!/bin/bash

mkdir -p data/
cd dbgen/

make clean
make

./dbgen -s 1
mv *.tbl ../data/

cd ../data/
for i in `ls *.tbl`
do
	sed 's/|$//' $i > ${i/tbl/csv}
	echo $i
done
