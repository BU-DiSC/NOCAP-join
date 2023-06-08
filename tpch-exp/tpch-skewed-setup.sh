#!/bin/bash

mkdir -p data/
cd dbgen/

sed -i 's/O_LCNT_MIN      4/O_LCNT_MIN      0/g' dss.h
sed -i 's/O_LCNT_MAX      28/O_LCNT_MAX      2000/g' dss.h
cp build-skew.c build.c

make clean
make
echo $1
./dbgen -f -s 1
sed -i 's/O_LCNT_MAX      2000/O_LCNT_MAX      28/g' dss.h
sed -i 's/O_LCNT_MIN      0/O_LCNT_MIN      4/g' dss.h
cp build-origin.c build.c

mv *.tbl ../data/

cd ../data/
for i in `ls *.tbl`
do
	echo $i
	#sed -i 's/|$//' $i
	mv $i ${i/tbl/csv}
done
#sort -R lineitem.csv > lineitem-tmp.csv
#mv lineitem-tmp.csv lineitem.csv
#sort -R orders.csv > orders-tmp.csv
#mv orders-tmp.csv orders.csv
cd ../
