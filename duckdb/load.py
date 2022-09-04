#!/usr/bin/env python3

import duckdb
import timeit
import psutil
import os

con = duckdb.connect(database="my-db.duckdb", read_only=False)
# See https://github.com/duckdb/duckdb/issues/3969
con.execute("PRAGMA threads={}".format(psutil.cpu_count()))

print("Will load the data")

start = timeit.default_timer()
con.execute(open("create.sql").read())
for fname in os.listdir('.'):
    if 'split_hits' not in fname:
        continue
    con.execute(f"COPY hits FROM '{fname}'")
end = timeit.default_timer()
print(end - start)
