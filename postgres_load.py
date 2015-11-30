#!/usr/bin/env python
#
# Tool for bulk loading gensort/sortbenchmark.org ascii tuples into PostgreSQL.
#
# This is tested to work on Linux only, and assumes that psql is in $PATH, and
# that gensort is in CWD.  It is also assumed that the files written to TMPDIR
# are readable by the Postgres OS user (typically because test builds are set
# up to have the same OS user as the hacker's system account).  This tool may
# well use non-portable shell conventions.
#
# The middle column, an ordinal number, is striped from original file before
# COPY processing.  This may imply that we end up with something that does not
# exactly match the requirements of certain "Daytona" category benchmarks (like
# Daytona Joulesort), but perfect compliance with the rules of the benchmark
# isn't a goal of this tool.  The requirement to produce files that are
# identical in format to the original (but in sorted order) is probably
# motivated only by verifiability for adjudicating the sort benchmark
# competition; users of this tool are also unlikely to write out the finished,
# fully sorted table contents in the original format once the sort is over
# (which is also a requirement).
#
# Table may be "skewed", which is useful for simulating a scenario where text
# abbreviated keys are less effective but still help somewhat.  This does not
# make much of a difference, because there is still plenty of entropy
# concentrated in the final 8 bytes (on 64-bit systems with 8 byte Datums).

import argparse
import os
import threading

# Each gensort_worker processes 10 million tuples per iteration:
tuples_per_iteration = 10L * 1000L * 1000L
# tmp directory for gensort/COPY temp files:
tmpdir = "/tmp"


def gensort_worker(worker_num, iteration, skew):
    """ Have worker process one iteration.

    An iteration is a (tuples_per_iteration tuples) slice of the total number
    of tuples stored in the final PostgreSQL table.  nthread workers are
    started at a time, with each processing one iteration.

    Keyword arguments:
        worker_num  -- ordinal identifier of worker thread
        iteration   -- iteration within sequence (starts from zero)
        skew        -- should tuple sortkey be "skewed"?
    """
    filename = "%s/it_%s" % (tmpdir, iteration)
    print 'worker %s generating file %s' % (worker_num, filename)
    os.system("./gensort -a " + ("-s " if skew else "") +
              # -b is starting point...
              "-b" + str(iteration * tuples_per_iteration) + " " +
              # ...always want this many tuples per worker iteration
              str(tuples_per_iteration) + " " + filename)

    print 'worker %s converting file %s to %s.copy' % (worker_num, filename,
                                                       filename)
    # Constants to make string interpolation to bash convenient:
    slash = '\\'
    bash_escape_slash = slash * 2
    bash_escape_replace = slash * 4
    tab = '\\t'
    # Used to strip line number, which is not stored:
    n_count_chars = '{32}'
    # Use sed substitution to convert to default PostgreSQL COPY format.  Must
    # escape \ characters appearing in sortkey, etc.
    os.system("cat " + filename + " | sed 's/" + bash_escape_slash + "/" +
              bash_escape_replace + "/g' | sed -E 's/[[:space:]]+[0-9A-F]" +
              n_count_chars + "[[:space:]][^$]/" + tab + bash_escape_replace +
              "x/g' > " + filename + ".copy")
    print 'worker %s deleting original file %s' % (worker_num, filename)
    os.system("rm " + filename)


def main(workers, skew, logged, collate, ntuples):
    """ Main function; starts and coordinates worker threads, performs COPY.

    Keyword arguments:
        workers  -- Total number of workers. Typically matches CPU core count.
        skew     -- should tuple sortkey be "skewed"?
        logged   -- should PostgreSQL table be logged?
        collate  -- sortkey column should use default collation?
        ntuples  -- final number of tuples required.
    """
    assert ntuples % tuples_per_iteration == 0, """ntuples (%s) is not
    evenly divisible by tuples_per_iteration
    (%s)""" % (ntuples, tuples_per_iteration)

    iteration = 0
    iterations = ntuples / tuples_per_iteration
    while iteration < iterations:
        threads = []
        for worker_num in range(1, workers + 1):
            t = threading.Thread(target=gensort_worker,
                                 args=(worker_num, iteration, skew, ))
            threads.append(t)
            t.start()
            iteration += 1
            if iteration == iterations:
                break
        # Wait for all worker threads to finish processing:
        for t in threads:
            t.join()

    # Do all work within single transaction, including creating new table.
    # This allows all bulk loading to use COPY FREEZE.
    #
    # Do not parallelize COPY.  Apart from being necessary to bulk load within
    # a single transaction, treating the ordering among partitions as special
    # ensures perfect determinism.  Having a recreatable test case is an
    # important goal of this tool.
    table = 'sort_test' if not skew else 'sort_test_skew'
    trans_sql = """psql -c "begin;
    drop table if exists %s;
    create %s table %s
    (
      sortkey text %s,
      payload bytea
    );\n""" % (
        table,
        '' if logged else 'unlogged',
        table,
        '' if collate else r'collate \"C\"')
    # Append COPY line to SQL string:
    for iteration in range(iterations):
        filename = "%s/it_%s.copy" % (tmpdir, iteration)
        trans_sql += "copy " + table + " from '" + filename + "' with freeze;\n"
    trans_sql += 'commit; checkpoint;"'

    print 'performing serial COPY of generated files'
    os.system(trans_sql)

    print 'deleting generated COPY format files'
    for iteration in range(iterations):
        filename = "%s/it_%s.copy" % (tmpdir, iteration)
        os.system("rm " + filename)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-w", "--workers", type=long,
                        help="Number of gensort workers", default=4)
    parser.add_argument("-m", "--million", type=long,
                        help="Generate n million tuples", default=100)
    parser.add_argument("-s", "--skew", action="store_true",
                        help="Skew distribution of output keys")
    parser.add_argument("-l", "--logged", action="store_true",
                        help="Use logged PostgreSQL table")
    parser.add_argument("-c", "--collate", action="store_true",
                        help="Use default collation rather than C collation")
    args = parser.parse_args()

    ntuples = args.million * 1000L * 1000L
    main(args.workers, args.skew, args.logged, args.collate, ntuples)
