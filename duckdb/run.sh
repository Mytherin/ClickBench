#!/bin/bash

TRIES=3

./server.sh start || exit 1

cat queries.sql | while read -r query; do
    sync
    echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null

    echo "$query";
    cli_params=()
    cli_params+=("-c")
    cli_params+=("CREATE OR REPLACE SECRET (TYPE quack, TOKEN 'clickbench')")
    cli_params+=("-c")
    cli_params+=("ATTACH 'quack:localhost' AS remote")
    cli_params+=("-c")
    cli_params+=("USE remote")
    cli_params+=("-c")
    cli_params+=(".timer on")
    for i in $(seq 1 $TRIES); do
      cli_params+=("-c")
      cli_params+=("${query}")
    done;
    echo "${cli_params[@]}"
    if ! duckdb "${cli_params[@]}"; then
      # the server crashed or was killed: report it and bring it back for the next query
      echo "Killed"
      ./server.sh stop
      ./server.sh start || exit 1
    fi
done;

./server.sh stop
