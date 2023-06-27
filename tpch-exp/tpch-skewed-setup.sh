#!/bin/bash

mkdir -p data/
cd dbgen/

sed -i 's/O_LCNT_MIN      1/O_LCNT_MIN      0/g' dss.h
sed -i 's/O_LCNT_MAX      7/O_LCNT_MAX      500/g' dss.h
cp build-skew.c build.c

make clean
make
echo $1
./dbgen -f -s 10
sed -i 's/O_LCNT_MAX      500/O_LCNT_MAX      7/g' dss.h
sed -i 's/O_LCNT_MIN      0/O_LCNT_MIN      1/g' dss.h
cp build-origin.c build.c

mv *.tbl ../data/

cd ../data/
for i in `ls *.tbl`
do
	echo $i
	sed -i 's/|$//' $i
	mv $i ${i/tbl/csv}
done
sort -R lineitem.csv > lineitem-tmp.csv
mv lineitem-tmp.csv lineitem.csv
sort -R orders.csv > orders-tmp.csv
mv orders-tmp.csv orders.csv

rm supplier.csv
rm nation.csv
rm customer.csv
rm part.csv
rm partsupp.csv
rm region.csv
cd ../
