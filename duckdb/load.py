#!/usr/bin/env python3

import duckdb
import timeit
import psutil

con = duckdb.connect(database="my-db.duckdb", read_only=False)


# enable the progress bar
con.execute('PRAGMA enable_progress_bar')
con.execute('PRAGMA enable_print_progress_bar;')
# enable parallel CSV loading
con.execute("SET experimental_parallel_csv=true")
# disable preservation of insertion order
con.execute("SET preserve_insertion_order=false")

# perform the actual load
print("Will load the data")
start = timeit.default_timer()
con.execute(open("create.sql").read())
con.execute("COPY hits FROM 'hits.csv'")
end = timeit.default_timer()
print(end - start)

# verify that the data was loaded correctly by running a number of verification queries
def compare_value(v1, v2):
    if type(v1) == type(0.0):
        # floating point comparison
        v2 = float(v2)
        lower_bound = v1 * 0.999
        upper_bound = v1 * 1.001
        # invert bounds for negative values
        if lower_bound > upper_bound:
            upper_bound,lower_bound = lower_bound,upper_bound
        return v2 >= lower_bound and v2 <= upper_bound
    return str(v1) == str(v2)

def compare_results(r1, r2, query_num, query):
    if len(r1) != len(r2):
        raise Exception(f"Row Count difference {len(r1)} - {len(r2)}")
    for row_num in range(len(r1)):
        lrow = r1[row_num]
        rrow = r2[row_num]
        if len(lrow) != len(rrow):
            raise Exception(f"Column Count difference {len(lrow)} - {len(rrow)}")
        for col_nom in range(len(lrow)):
            if not compare_value(lrow[col_nom], rrow[col_nom]):
               print(type(lrow[col_nom]))
               print(type(rrow[col_nom]))
               raise Exception(f"Value difference {lrow[col_nom]} - {rrow[col_nom]} in column {col_nom} of query {query_num}:\n{query}")


print("Verifying data load")
# queries
with open('verify_load.sql') as f:
    query_lines = f.readlines()
# answers
verification_file = 'answers/verification_answers.csv'
verification_results = con.execute(f"""
SELECT *
FROM read_csv_auto('{verification_file}', all_varchar=True, header=False)
""").fetchall()
query_count = len(query_lines)
assert len(verification_results) == query_count

for i in range(query_count):
    query = query_lines[i]
    print(f'{i + 1}/{query_count}')
    # run the verification query
    results = con.execute(query).fetchall()
    # compare the results
    compare_results(results, [verification_results[i]], i + 1, query)
