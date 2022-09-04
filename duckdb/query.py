#!/usr/bin/env python3

import duckdb
import timeit
import psutil
import sys

query = sys.stdin.read()
print(query)

con = duckdb.connect(database="my-db.duckdb", read_only=False)
con.execute("PRAGMA threads={}".format(psutil.cpu_count()))

for try_num in range(3):
    start = timeit.default_timer()
    res = con.execute(query).df()
    end = timeit.default_timer()
    print(end - start)
    print(res)
    del res
