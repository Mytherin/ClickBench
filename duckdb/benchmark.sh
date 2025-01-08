#!/bin/bash

# Install
sudo apt-get update
sudo apt-get install ninja-build cmake build-essential make ccache pip -y

git clone https://github.com/Mytherin/duckdb
cd duckdb
git checkout archnative
NATIVE_ARCH=1 LTO=thin make
export PATH="$PATH:`pwd`/build/release/"
cd ..

# Load the data
wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

time duckdb hits.db -f create.sql -c "COPY hits FROM 'hits.csv'"

# Run the queries

./run.sh 2>&1 | tee log.txt

wc -c hits.db

cat log.txt |
  grep -P '^Run Time \(s\): real' |
  sed -r -e 's/^Run Time \(s\): real\s*([0-9.]+).*$/\1/' |
  awk '{ if ($2) { print $1 * 60 + $2 } else { print $1 } }' |
  awk '{ if ($1 == "null") { skip = 1 } else { if (i % 3 == 0) { printf "[" }; printf skip ? "null" : $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; skip = 0; } }'
