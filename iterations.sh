#!/bin/bash
#
# Convenience script for testing multiple CREATE INDEX cases.
#
# This is intended to be run like this:
#
# ./iterations.sh 2>&1 | tee test_output.txt

for i in 50 100 250 500
do
	echo "Bulk loading $i million tuples"
	# We could be cleverer about reusing tuples across iterations, but
	# don't bother for now.
	./postgres_load.py -m $i -w 16
	date
        psql <<EOF
\x
\set VERBOSITY default
set trace_sort = on;
set client_min_messages = 'LOG';
set maintenance_work_mem = '1GB';
create index index_test on sort_test(sortkey);
drop index index_test;
create index index_test on sort_test(sortkey);
drop index index_test;
create index index_test on sort_test(sortkey);
drop index index_test;
EOF
done
