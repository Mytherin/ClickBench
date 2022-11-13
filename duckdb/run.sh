#!/bin/bash

QUERY_NUM=1
cat queries.sql | while read query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    ./query.py --number=${QUERY_NUM} <<< "${query}"

    QUERY_NUM=$((QUERY_NUM + 1))
done
