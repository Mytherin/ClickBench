import duckdb
import sys

def sanitize_value(v):
    # several characters are escaped by ClickHouse when writing to CSV
    # convert them back to literals
    # form feed characters (\f)
    v = v.replace('\\f', '\f')
    # single quotes (')
    v = v.replace("\\'", "'")

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
    return sanitize_value(str(v1)) == sanitize_value(str(v2))

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
                r1_type = str(type(lrow[col_nom]))
                r2_type = str(type(rrow[col_nom]))
                print(f"Value difference actual \"{lrow[col_nom]}\" (type {r1_type}) - expected \"{rrow[col_nom]}\" (type {r2_type}) in row {row_num + 1} column {col_nom + 1} of query {query_num}:\n{query}")

                r1_str = ['\t'.join([str(y) for y in x]) for x in r1]
                r2_str = ['\t'.join([str(y) for y in x]) for x in r2]
                print("==============")
                print("Actual Results")
                print("==============")
                for entry in r1_str:
                    print(entry)
                print("==============")
                print("Expected Results")
                print("==============")
                for entry in r2_str:
                    print(entry)
                raise Exception("Incorrect value encountered")

def verify(results, query, qnum):
    # these queries do not have deterministic results - we cannot verify them
    # note these indexes are 1-based (18=Q17 on the website, etc)
    # Q18: No ORDER BY after a group
    # Q22: ORDER BY has ties (row 9-10 both have "7" as COUNT)
    # Q31: ORDER BY has ties (row 10-11 both have "1058" as COUNT)
    # Q32: ORDER BY has ties (all 10 rows in the result have a count of "1")
    # Q33: ORDER BY has ties (all 10 rows in the result have a count of "1" or "2")
    skip_list = [18, 22, 31, 32, 33]
    if qnum in skip_list:
        return
    con = duckdb.connect()
    expected_results = con.execute(f"""
    SELECT *
    FROM read_csv_auto('answers/q{qnum}.csv', all_varchar=True, header=False, nullstr='NULL')
    """).fetchall()
    compare_results(results, expected_results, qnum, query)


