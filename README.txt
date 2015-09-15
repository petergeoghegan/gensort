Gensort and Valsort Source Code Distribution

What's New in Version 1.5?
----------------------------
- The "-s" option has been added to the gensort program. This option causes
  moderately skewed input keys to be generated.
- The valsort program is unchanged, except its version number is now 1.5.

Contents
--------
  Source code and associated include files:
      gensort.c
      valsort.c
      sump.c
      sump.h 
      sump_win.c    (necessary only on Windows)
      sump_win.h    (necessary only on Windows)
      sumpversion.h
      rand16.c
      rand16.h

   Makefile and supporting files:
      Makefile
      Make.win

   GNU GPL 2.0 License
      gpl-2.0.txt

   Zlib files:      (necessary only on Windows)
      zdll.lib
      zlib1.dll
      zlib/include/zlib.h
      zlib/include/zconf.h

Directions
----------
   On Linux:
      To build gensort and valsort:
         make

   On Windows 
      To build gensort and valsort:
      (For nmake commands, must be inside an x86 Visual Studio Command 
      Prompt window.)
         nmake -f Make.win
      If you move gensort.exe or valsort.exe to a different directory,
      you need to make sure the zlib1.dll file is either in the same
      directory or in a directory Windows will search for x86 dll's.

Extracting PostgreSQL COPY-able columns on Linux
------------------------------------------------

This escapes to make input copy safe, does not store ordinal number column, and
stores payload data as bytea, since it's clearly supposed to be binary data:

cat pennytest | sed 's/\\/\\\\/g' | sed -E 's/[[:space:]]+[0-9A-F]+[[:space:]][^$]/\t\\\\x/g' > copy.input

Then:

postgres=# create table test (a text , b bytea);
CREATE TABLE
postgres=# copy test from '/home/pg/gensort/copy.input';
COPY 100
