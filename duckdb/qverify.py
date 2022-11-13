import duckdb
import sys

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

def verify(results, query, qnum):
	if len(sys.args) < 2:
		print("Expected ./query --number=X"
		exit(1)
	qnum = None
	for arg in sys.args:
		if arg.startswith('--number'):
			qnum = int(arg.replace('--number', ''))
	if qnum is None:
		print("Expected ./query --number=X"
		exit(1)
	con = duckdb.connect()
	expected_results = con.execute(f"""
	SELECT *
	FROM read_csv_auto('answers/q{qnum}.csv', all_varchar=True)
	""").fetchall()
	compare_results(results, expected_results, qnum, query)


