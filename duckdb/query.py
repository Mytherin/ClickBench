#!/usr/bin/env python3

import duckdb
import timeit
import psutil
import sys
import qverify

if len(sys.argv) < 2:
    print("Expected ./query --number=X")
    exit(1)
qnum = None
for arg in sys.argv:
    if arg.startswith('--number='):
        qnum = int(arg.replace('--number=', ''))
if qnum is None:
    print("Expected ./query --number=X")
    exit(1)

query = sys.stdin.read()
print(query)

con = duckdb.connect(database="my-db.duckdb", read_only=False)

for try_num in range(3):
    start = timeit.default_timer()
    results = con.execute(query).fetchall()
    end = timeit.default_timer()
    print(end - start)

    qverify.verify(results, query, qnum)
