#!/bin/bash
wget http://homepages.cwi.nl/~boncz/job/imdb.tgz
tar -xvf imdb.tgz

../build/data-converter --CSV2DAT --right-table-input-path=cast_info.csv --right-table-output-path=workload-rel-S.dat --left-table-input-path=title.csv --left-table-output-path=workload-rel-R.dat --sep=,
