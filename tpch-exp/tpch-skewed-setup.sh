#!/bin/bash

mkdir -p data/
cd dbgen/

sed -i 's/O_LCNT_MIN      1/O_LCNT_MIN      0/g' dss.h
sed -i 's/O_LCNT_MAX      7/O_LCNT_MAX      400/g' dss.h
cp rnd-skew.c rnd.c

make clean
make
echo $1
./dbgen -f -s 1
sed -i 's/O_LCNT_MAX      400/O_LCNT_MAX      7/g' dss.h
sed -i 's/O_LCNT_MIN      0/O_LCNT_MIN      1/g' dss.h
cp rnd-origin.c rnd.c

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
cd ../
