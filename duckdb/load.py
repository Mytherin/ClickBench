#!/usr/bin/env python3

import duckdb
import timeit
import psutil

con = duckdb.connect(database="my-db.duckdb", read_only=False)

print("Will load the data")

start = timeit.default_timer()
con.execute(open("create.sql").read())
con.execute("SET experimental_parallel_csv=true")
con.execute("SET preserve_insertion_order=false")
con.execute("COPY hits FROM 'hits.csv'")
end = timeit.default_timer()
print(end - start)
