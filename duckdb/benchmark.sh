#!/bin/bash

# Install

sudo apt-get update
sudo apt-get install -y python3-pip
pip install duckdb psutil pandas

# Load the data

wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz
split -x -l 10000000 hits.csv split_hits

./load.py

# Run the queries

./run.sh 2>&1 | tee log.txt

wc -c my-db.duckdb

cat log.txt | grep -P '^\d|Killed|Segmentation' | sed -r -e 's/^.*(Killed|Segmentation).*$/null\nnull\nnull/' |
    awk '{ if (i % 3 == 0) { printf "[" }; printf $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; }'
