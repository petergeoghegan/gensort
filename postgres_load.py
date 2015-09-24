#!/usr/bin/env python
#
# Tool for bulk loading gensort "Daytona Sort" category data into PostgreSQL.
#
# This is tested to work on Linux, assumes that psql is in $PATH, and that
# gensort is in CWD.  It may well use non-portable shell conventions.
#
# Line number is striped from original file before COPY processing .  This may
# imply that we end up with something that does not exactly match the
# requirements of the Daytona category benchmark, but perfect compliance with
# the rules of the benchmark isn't really a goal of this tool.
#
# Table may be "skewed", which is useful for simulating a scenario where text
# abbreviated keys are less effective but still help somewhat.

import argparse
import os
import threading

# Each gensort_worker processes 10 million tuples per iteration:
tuples_per_iteration = int(float('1e7'))


def gensort_worker(worker_num, iteration, skew):
    filename = "/tmp/it_%s" % iteration
    print 'worker %s generating file %s' % (worker_num, filename)
    os.system("./gensort -a " + ("-s " if skew else "") +
              # -b is starting point...
              "-b" + str(iteration * tuples_per_iteration) + " " +
              # ...always want this many tuples per worker iteration
              str(tuples_per_iteration) + " " + filename)
    print 'worker %s converting file %s to COPY format' % (worker_num,
                                                           filename)

    # Constants to make string interpolation to bash convenient:
    slash = '\\'
    bash_escape_slash = slash * 2
    bash_escape_replace = slash * 4
    tab = '\\t'
    # Used to strip line number, which is not stored
    n_count_chars = '{32}'
    os.system("cat " + filename + " | sed 's/" + bash_escape_slash + "/" +
              bash_escape_replace + "/g' | sed -E 's/[[:space:]]+[0-9A-F]" +
              n_count_chars + "[[:space:]][^$]/" + tab + bash_escape_replace +
              "x/g' > " + filename + ".copy")
    os.system("rm " + filename)


def main(nthreads, skew, logged, ntuples):
    # Create new table for benchmarking
    tablename = 'sort_test' if not skew else 'sort_test_skew'
    os.system('psql -c "drop table if exists ' + tablename + '"')
    os.system('psql -c "create ' + ('unlogged ' if not logged else '') +
              'table ' + tablename + '(sortkey text, payload bytea);"')

    assert(ntuples % tuples_per_iteration == 0)
    iterations = ntuples / tuples_per_iteration
    iteration = 0
    while iteration < iterations:
        threads = []
        for i in range(nthreads):
            t = threading.Thread(target=gensort_worker,
                                 args=(i, iteration, skew, ))
            threads.append(t)
            t.start()
            iteration += 1
            if iteration == iterations:
                break

        # Wait for all worker threads to finish processing:
        for t in threads:
            t.join()

    # Do not parallelize COPY.  Treat the ordering among runs as special, to
    # ensure perfect determinism.
    iteration = 0
    while iteration < iterations:
        filename = "/tmp/it_%s.copy" % iteration
        os.system('psql -c "copy ' + tablename + ' from \'' + filename + '\' "')
        os.system("rm " + filename)
        iteration += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-w", "--workers", type=int,
                        help="Number of gensort workers", default=4)
    parser.add_argument("-m", "--million", type=int,
                        help="Generate n million tuples", default=100)
    parser.add_argument("-s", "--skew", action="store_true",
                        help="Skew distribution of output keys")
    parser.add_argument("-l", "--logged", action="store_true",
                        help="Use logged PostgreSQL table")
    args = parser.parse_args()

    ntuples = args.million * 1000000
    main(args.workers, args.skew, args.logged, ntuples)
