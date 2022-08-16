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
sort -R lineitem.csv > lineitem-tmp.csv
sort -R orders.csv > orders-tmp.csv
mv lineitem-tmp.csv lineitem.csv
mv orders-tmp.csv orders.csv
cd -
