#!/bin/bash
# Start or stop a long-lived DuckDB server (quack) on hits.db so its buffer cache stays warm across queries.

export HOME=${HOME:=~}
export PATH=$HOME'/.duckdb/cli/latest':$PATH

case "$1" in
start)
    # sleep keeps the CLI's stdin open, so the process (and the quack server in it) keeps running
    nohup bash -c "sleep infinity | duckdb hits.db -cmd \"CALL quack_serve('quack:localhost', token='clickbench')\"" > server.log 2>&1 &
    echo $! > server.pid
    for i in $(seq 1 120); do
        if duckdb -c "CREATE OR REPLACE SECRET (TYPE quack, TOKEN 'clickbench')" -c "ATTACH 'quack:localhost' AS remote" > /dev/null 2>&1; then
            exit 0
        fi
        sleep 1
    done
    echo "quack server did not start" 1>&2
    cat server.log
    exit 1
    ;;
stop)
    if [ -f server.pid ]; then
        pkill -P "$(cat server.pid)"
        kill "$(cat server.pid)" 2> /dev/null
        rm -f server.pid
    fi
    ;;
*)
    echo "usage: $0 start|stop" 1>&2
    exit 1
    ;;
esac
