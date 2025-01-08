#!/bin/bash

# Install
wget https://github.com/Mytherin/duckdb/tree/archnative
cd duckdb
NATIVE_ARCH=1 LTO=full make
alias duckdb = `pwd`/build/release/duckdb
cd ..

# Load the data
wget --no-verbose --continue 'https://datasets.clickhouse.com/hits_compatible/hits.csv.gz'
gzip -d hits.csv.gz

time duckdb hits.db -f create.sql -c "COPY hits FROM 'hits.csv'"

# Run the queries

./run.sh 2>&1 | tee log.txt

wc -c hits.db

cat log.txt |
  grep -P '^real|^Error' |
  sed -r -e 's/^Error.*$/null/; s/^real\s*([0-9.]+)m([0-9.]+)s$/\1 \2/' |
  awk '{ if ($2) { print $1 * 60 + $2 } else { print $1 } }' |
  awk '{ if ($1 == "null") { skip = 1 } else { if (i % 3 == 0) { printf "[" }; printf skip ? "null" : $1; if (i % 3 != 2) { printf "," } else { print "]," }; ++i; skip = 0; } }'
