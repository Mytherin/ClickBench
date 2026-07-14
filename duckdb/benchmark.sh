#!/bin/bash
export BENCH_DOWNLOAD_SCRIPT="download-hits-parquet-single"
# DuckDB now runs as a persistent "quack" client/server: ./start launches a
# long-lived server that holds hits.db resident, and ./query connects to it as a
# client. This makes stop/start meaningful, so the driver's cold cycle (./stop ->
# drop_caches -> ./start) restarts the server only for the cold try of each
# query; the warm tries reuse the already-running, cache-warm server.
export BENCH_RESTARTABLE=yes
# Data lives on disk in hits.db and survives a server restart — no reload needed
# between queries.
export BENCH_DURABLE=yes
# The quack server is a genuine shared server that many clients can hit at once,
# so the concurrent-QPS throughput test is meaningful again (it was skipped for
# the old embedded, fork-per-query setup; see issue #946).
exec ../lib/benchmark-common.sh
