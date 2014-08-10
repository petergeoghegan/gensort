/* sump.h - SUMP Pump(TM) SMP/CMP parallel data pump library
 *          SUMP Pump is a trademark of Ordinal Technology Corp
 *
 * SUMP (Scale Up MultiProcessor) Pump is an executable program and
 * subroutine library that allows programmers to quickly take advantage
 * of multiple processors in a computer system. The sump pump program,
 * sump, can run multiple instances of a separate program to parallelize
 * its processing.  The sump pump library allows programmers to write
 * subroutines that are automatically parallelized by the sump pump
 * infrastructure.
 *
 * $Revision: 122 $
 *
 * Copyright (C) 2010 - 2011, Ordinal Technology Corp, http://www.ordinal.com
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of Version 2 of the GNU General Public
 * License as published by the Free Software Foundation.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 *
 * Linking SUMP Pump statically or dynamically with other modules is
 * making a combined work based on SUMP Pump.  Thus, the terms and
 * conditions of the GNU General Public License v.2 cover the whole
 * combination.
 *
 * In addition, as a special exception, the copyright holders of SUMP Pump
 * give you permission to combine SUMP Pump program with free software
 * programs or libraries that are released under the GNU LGPL and with
 * independent modules that communicate with SUMP Pump solely through
 * Ordinal Technology Corp's Nsort Subroutine Library interface as defined
 * in the Nsort User Guide, http://www.ordinal.com/NsortUserGuide.pdf.
 * You may copy and distribute such a system following the terms of the
 * GNU GPL for SUMP Pump and the licenses of the other code concerned,
 * provided that you include the source code of that other code when and
 * as the GNU GPL requires distribution of source code.
 *
 * Note that people who make modified versions of SUMP Pump are not
 * obligated to grant this special exception for their modified
 * versions; it is their choice whether to do so.  The GNU General
 * Public License gives permission to release a modified version without
 * this exception; this exception also makes it possible to release a
 * modified version which carries forward this exception.
 *
 * For more information on SUMP Pump, see:
 *     http://www.ordinal.com/sump.html
 *     http://code.google.com/p/sump-pump/
 */

#ifndef _SUMP_PUMP_H_
#define _SUMP_PUMP_H_

#include <stdio.h>
#include <sys/types.h>
#if defined(_WIN32)
# define win_nt 1
typedef __int64 int64_t;
typedef unsigned __int64 uint64_t;
# include "sump_win.h"
#else   /* now non-Windows */
# include <stdint.h>
#endif

/* sump pump type */
typedef struct sump *sp_t;

/* sump pump param type */
typedef struct sp_param *sp_param_t;

/* sump pump task type that is passed to pump functions */
typedef struct sp_task *sp_task_t;

/* sump pump file type */
typedef struct sp_file *sp_file_t;

/* sump pump link type */
typedef struct sp_link *sp_link_t;

/* pump function type.  this is the function that is executed
 * in parallel by multiple sump pump member threads
 */
typedef int (*sp_pump_t)(sp_task_t t, void *arg);

/* SUMP Pump Library
 * -----------------
 *
 * The sump pump library facilitates the automatic parallelization of
 * either (1) a programmer-defined function that reads input records and
 * writes output records (a pump function or pump func), or (2) an
 * external program that reads its standard input and writes to its
 * standard output.
 *
 * The sump pump API consists of functions that can be called by the
 * host program, and the functions that can be called by the pump
 * functions.
 */

/* Library Functions for Host Program
 * ----------------------------------
 */

/* sp_get_version - get the subversion version for sump pump
 *
 * Returns: a string containing the subversion version. The string should
 *          NOT be free()'d.
 */
const char *sp_get_version(void);


/* sp_get_id - get the subversion id keyword substitution for sump pump
 *
 * Returns: a string containing the subversion id keyword. The string should
 *          NOT be free()'d.
 */
const char *sp_get_id(void);


/* sp_start - Start a sump pump
 *
 * Parameters:
 *      sp -          Pointer to where to return newly allocated sp_t 
 *                    identifier that will be used in as the first argument
 *                    to all subsequent sp_*() calls.
 *      pump_func -   Pointer to pump function that will be called by
 *                    multiple sump pump threads at once. If an external
 *                    program name is specified in the arg_fmt string, this
 *                    parameter can and must be NULL.
 *      arg_fmt -     Printf-format-like string that can be used to specify
 *                    the following sump pump directives:
 *                    -ASCII or -UTF_8    Input records are ascii/utf-8 
 *                                        characters delimited by a newline
 *                                        character.
 *                    -GROUP_BY or -GROUP Group input records for the purpose
 *                                        of reducing them. The sump pump input
 *                                        should be coming from an nsort
 *                                        instance where the "-match"
 *                                        directive has been declared. This
 *                                        directive prevents records with
 *                                        equal keys from being dispersed to
 *                                        more than one sump pump task.
 *                    -IN=%s or -IN_FILE=%s Input file name for the sump pump
 *                                        input.  if not specified, the input
 *                                        should be written into the sump pump
 *                                        either by calls to sp_write_input()
 *                                        or sp_start_link()
 *                                        The input file name can be followed
 *                                        by options that control the file
 *                                        access mode and transfer size.
 *                                        See sp_open_file_src() comments.
 *                    -IN_BUF_SIZE=%d{k,m,g} Overrides default input buffer
 *                                        size (256kb). If a 'k', 'm' or 'g'
 *                                        suffix is specified, the specified
 *                                        size is multiplied by 2^10, 2^20 or
 *                                        2^30 respectively.
 *                    -IN_BUFS=%d         Overrides default number of input
 *                                        buffers (the number of tasks).
 *                    -OUT[%d]=%s or -OUT_FILE[%d]=%s  The output file name for
 *                                        the specified output index, or output
 *                                        0 if no index is specified.  If not 
 *                                        defined, the output should be read
 *                                        either by calls to sp_read_output()
 *                                        or by sp_start_link().
 *                                        The output file name can be followed
 *                                        by options that control the file
 *                                        access mode and transfer size.
 *                                        See sp_open_file_dst() comments.
 *                    -OUT_BUF_SIZE[%d]=%d{x,k,m,g} Overrides default output
 *                                        buffer size (2x the input buf size)
 *                                        for the specified output index, or
 *                                        output 0 if no index is specified.
 *                                        If the size ends with a suffix of
 *                                        'x', the size is used as a multiplier
 *                                        of the input buffer size. If a 'k',
 *                                        'm' or 'g' suffix is specified, the
 *                                        specified size is multiplied by 2^10,
 *                                        2^20 or 2^30 respectively. It is not
 *                                        an error if the output of a task
 *                                        exceeds the output buffer size, but
 *                                        it can potentially result in loss
 *                                        of parallelism.
 *                    -OUTPUTS=%d         Overrides default number of output
 *                                        streams (1).
 *                    -REC_SIZE=%d        Defines the input record size in 
 *                                        bytes. The record contents need not 
 *                                        be ascii nor delimited by a newline
 *                                        character. If not specified, records
 *                                        must consist of ascii or utf-8
 *                                        characters and be terminated by a
 *                                        newline.
 *                    -TASKS=%d           Overrides default number of output
 *                                        tasks (3x the number of threads).
 *                    -THREADS=%d         Overrides default number of threads
 *                                        that are used to execute the pump
 *                                        function in parallel. The default is
 *                                        the number of logical processors in
 *                                        the system.
 *                    -WHOLE or           Processing is not done by input
 *                      -WHOLE_BUF        records so not input record type
 *                                        should be defined.  Instead,
 *                                        processing is done by whole input
 *                                        buffers.
 *                    -DEFAULT_FILE_MODE={BUFFERED,BUF,DIRECT,DIR}  Set the
 *                                        default file access mode for both
 *                                        input and output files.  If none is
 *                                        specified, the direct mode is used
 *                                        to access input and output files for
 *                                        which a BUFFERED file modifier is
 *                                        not specified.
 *                    [external_program_name external_program_arguments]
 *                                        The name of an external program and
 *                                        its arguments. The program name
 *                                        cannot start with the '-' character.
 *                                        The external program name and its 
 *                                        arguments must be last in the
 *                                        arg_fmt string. If the program name
 *                                        does not include a path, the PATH
 *                                        environment variable will be used
 *                                        to find it.
 *      ...           potential subsequent arguments to arg_fmt
 *
 * Returns: SP_OK or a sump pump error code
 *
 * The sump pump directives can also appear in a SUMP_PUMP environment
 * variable.
 */
int sp_start(sp_t *sp, sp_pump_t pump_func, char *arg_fmt, ...);


/* sp_argv_to_str - bundle up the specified argv and return it as a string
 *                  for instance if argc is 2, argv[0] is "TASKS=2" and
 *                  argv[1] is "THEADS=3", then return the string
 *                  "TASKS=2 THREADS=3".
 *
 * Returns: a string containing the command line arguments passed as function
 *          arguments. The string should be free()'d when no longer needed.
 */
char *sp_argv_to_str(char *argv[], int argc);


/* sp_start_sort - start an nsort instance with a sump pump outside
 *                 wrapper so that its input or output can be assigned to
 *                 a file or linked to another sump pump.  For instance,
 *                 the sort input or output can be linked to sump pumps
 *                 performing record pumping such as "map" on input
 *                 and "reduce" for output.
 * Parameters:
 *      sp -       Pointer to where to return newly allocated sp_t 
 *                 identifier that will be used in as the first
 *                 argument to all subsequent sp_*() calls.
 *      def -      Nsort sort definition string.  Besides the Nsort 
 *                 commands listed in the Nsort User Guide, the following
 *                 directives are also recognized:
 *                   -match[=%d] Each output record will be preceded by a
 *                               single byte that indicates the
 *                               specified number of keys in this record
 *                               are the same as in the previous record.
 *                               If no key number is specified, all keys
 *                               are examined for a match condition.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_start_sort(sp_t *sp,
                  char *def_fmt,
                  ...);


/* sp_get_sort_stats - get a string containing the nsort statistics report
 *                     for an nsort sump pump that has completed.
 *
 * Returns: a string containing the Nsort statistics report. The string should
 *          NOT be free()'d and is valid until the passed sp_t is sp_free()'d.
 */
const char *sp_get_sort_stats(sp_t sp);


/* sp_get_nsort_version - get the subversion version for sump pump
 *
 * Returns: a string containing the Nsort version number. The string should
 *          NOT be free()'d.
 */
const char *sp_get_nsort_version(void);


/* sp_link - start a link/connection between an output
 *           of a sump pump to the input of another sump pump.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_link(sp_t out_sp, unsigned out_index, sp_t in_sp);


/* sp_write_input - write data that is the input to a sump pump.
 *                  A write size of 0 indicates input EOF.
 *
 * Returns: the amount of data written. If this is not the same as the
 *          requested write size, an error has occurred.
 */
ssize_t sp_write_input(sp_t sp, void *buf, ssize_t size);


/* sp_get_in_buf - get a pointer to an input buffer that an external
 *                 thread can fill with input data.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_get_in_buf(sp_t sp, uint64_t index, void **buf, size_t *size);


/* sp_put_in_buf_bytes - flush bytes that have been placed in a sump
 *                       pump's input buffer by an external thread.
 *                       This function should only be used by first
 *                       calling sp_get_in_buf().
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_put_in_buf_bytes(sp_t sp, uint64_t index, size_t size, int eof);


/* sp_read_output - read bytes from a specified output of the specified
 *                  sump pump.
 *
 * Returns: The number of bytes read.  If 0, then EOF has occurred.
 *          If negative, an error has occurred.
 */
ssize_t sp_read_output(sp_t sp, unsigned index, void *buf, ssize_t size);


/* sp_get_error - get the error code of a sump pump.
 *
 * Returns: SP_OK if no error has occurred, otherwise the error code.
 */
int sp_get_error(sp_t sp);


/* sp_wait - can be called by an external thread, e.g. the thread that
 *           called sp_start(), to wait for all sump pump activity to cease.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_wait(sp_t sp);


/* sp_open_file_src - use the specified file as the input for the
 *                    specified sump pump.
 *
 * Parameters:
 *      sp -          sp_t identifier for which we are opening the input file.
 *      fname_mods -  Name of the file, potentially followed by one or more
 *                    of the following modifiers (with no intervening spaces):
 *                    ,BUFFERED or ,BUF The file will be read with normal
 *                                      buffered (not direct) reads.
 *                    ,DIRECT or ,DIR   The file will be read with direct
 *                                      and asynchronous reads.
 *                    ,TRANSFER=%d{k,m,g} or ,TRANS=%d{k,m,g} or ,TR=%d{k,m,g}
 *                                      The transfer size (read request size)
 *                                      is specified in kilo, mega or giga
 *                                      bytes.
 *                    ,COUNT=%d or ,CO=%d  The count of the maximum number of
 *                                      outstanding asynchronous read requests
 *                                      is given.
 *                    Example:
 *                       myfilename,dir,trans=4m,co=4
 *                                      The above example specifies a file
 *                                      name of "myfilename", with direct
 *                                      and asynchronous reads, with a
 *                                      request size of 4 MB, and a maximum
 *                                      of 4 outstanding asynchronous read
 *                                      requests at any time.
 *
 * Returns: NULL if an error occurs in opening the file, otherwise
 *          a valid sump pump file structure.
 */
sp_file_t sp_open_file_src(sp_t sp, const char *fname_mods);


/* sp_open_file_dst - use the specified file as the output for the
 *                    specified output of the specified sump pump.
 *
 * Parameters:
 *      sp -          sp_t identifier for which we are opening an output file.
 *      out_index -   Integer indicating the sump pump output index.
 *      fname_mods -  Name of the file, potentially followed by one or more
 *                    of the following modifiers (with no intervening spaces):
 *                    ,BUFFERED or ,BUF The file will be written with normal
 *                                      buffered (not direct) writes.
 *                    ,DIRECT or ,DIR   The file will be written with direct
 *                                      and asynchronous writes.
 *                    ,TRANSFER=%d{k,m,g} or ,TRANS=%d{k,m,g} or ,TR=%d{k,m,g}
 *                                      The transfer size (write request size)
 *                                      is specified in kilo, mega or giga
 *                                      bytes.
 *                    ,COUNT=%d or ,CO=%d  The count of the maximum number of
 *                                      outstanding asynchronous write requests
 *                                      is given.
 *                    Example:
 *                       myfilename,dir,trans=4m,co=4
 *                                      The above example specifies a file
 *                                      name of "myfilename", with direct
 *                                      and asynchronous writes, with a
 *                                      request size of 4 MB, and a maximum
 *                                      of 4 outstanding asynchronous write
 *                                      requests at any time.
 *
 * Returns: NULL if an error occurs in opening the file, otherwise
 *          a valid sump pump file structure.
 */
sp_file_t sp_open_file_dst(sp_t sp, unsigned out_index, const char *fname_mods);


/* sp_file_wait - wait for the specified file connection to complete.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_file_wait(sp_file_t sp_file);


/* sp_get_error_string - return an error message string
 *
 * Returns: a string containing a sump pump error message. The string should
 *          NOT be free()'d and is valid until the passed sp_t is sp_free()'d.
 */
const char *sp_get_error_string(sp_t sp, int error_code);


/* sp_free - free the specified sp_t and its associated state.
 */
void sp_free(sp_t *caller_sp);


/* sp_file_free - free the specified sp_file_t and its associated state.
 */
void sp_file_free(sp_file_t *caller_sp_file);


/* Library Functions for Pump Functions
 * ------------------------------------
 *
 * The API calls specific to pump functions all take as an argument the
 * sp_task_t data type that is passed to the pump function by the sump
 * pump infrastructure.
 *
 * "Make my funk the P.Funk, I wants to get funked up." - George Clinton
 */

/* pfunc_get_rec - get a pointer to the next input record for a pump function.
 *                 The sump pump infrastructure allocates the record buffers
 *                 and modifies the pointer-to-a-pointer argument to point to
 *                 the buffer. If the record type is text, a null character
 *                 will terminate the record.
 *
 * Returns: 0 if no more records are in the sump pump task input, otherwise
 *          the number bytes in the record not including the terminating null
 *          character.
 */
size_t pfunc_get_rec(sp_task_t t, void *ptr_to_rec_ptr);


/* pfunc_get_in_buf - get a pointer to the input buffer for a pump function.
 *
 * Returns: SP_OK or a sump pump error code
 */
int pfunc_get_in_buf(sp_task_t t, void **buf, size_t *size);


/* pfunc_get_out_buf - get a pointer to an output buffer and its size for
 *                     a pump function.
 *
 * Returns: SP_OK or a sump pump error code
 */
int pfunc_get_out_buf(sp_task_t t, unsigned out_index, void **buf, size_t *size);


/* pfunc_put_out_buf_bytes - flush bytes that have been placed in a pump
 *                           function's output buffer.  This routine can only
 *                           be used by first calling pf_get_out_buf().
 *
 * Returns: SP_OK or a sump pump error code
 */
int pfunc_put_out_buf_bytes(sp_task_t t, unsigned out_index, size_t size);


/* pfunc_get_thread_index - can be used by pump functions to get the
 *                          index of the sump pump thread executing the
 *                          pump func.  For instance, if there are 4
 *                          sump pump threads, this function will return
 *                          0-3 depending on which of the 4 threads is
 *                          invoking it.
 *
 * Returns: The index of the requesting sump pump thread.
 */
int pfunc_get_thread_index(sp_task_t t);


/* pfunc_get_task_number - can be used by pump functions to get the sump
 *                         pump task number being executed by the pump
 *                         function.  This number starts at 0 and
 *                         increases with each subsequent task
 *                         issued/started by the sump pump.
 *
 * Returns: The task number (starting with 0 as the first task) that the
 *          calling thread is currently executing.
 */
uint64_t pfunc_get_task_number(sp_task_t t);


/* pfunc_write - write function that can be used by a pump function to
 *               write the output data for the pump function.
 *
 * Returns: the number of bytes written.  If this is not the same as the
 *          requested size, an error has occurred.
 */
size_t pfunc_write(sp_task_t t, unsigned out_index, void *buf, size_t size);


/* pfunc_printf - print a formatted string to a pump functions's output.
 *
 * Returns: the number of bytes written.  If this is not the same as the
 *          requested size, an error has occurred.
 */
int pfunc_printf(sp_task_t t, unsigned out_index, const char *fmt, ...);


/* pfunc_error - raise an error for a pump function and define an error 
 *               string that can be retrived by other threads using
 *               sp_get_error_string().
 *
 * Returns: SP_PUMP_FUNCTION_ERROR
 */
int pfunc_error(sp_task_t t, const char *fmt, ...);


/* pfunc_mutex_lock - lock the auto-allocated mutex for pump functions.
 */
void pfunc_mutex_lock(sp_task_t t);


/* pfunc_mutex_unlock - unlock the auto-allocated mutex for pump functions.
 */
void pfunc_mutex_unlock(sp_task_t t);


/* Sump Pump Library Error Codes
 * -----------------------------
 *
 * Many sump pump API call return an integer indicates one the following
 * success or error conditions:
 */

/* SP_OK - no error occurred, everything OK */
#define SP_OK                   0

/* SP_FILE_READ_ERROR - a file read error occurred */
#define SP_FILE_READ_ERROR      (-1)

/* SP_FILE_WRITE_ERROR - a file write error occurred */
#define SP_FILE_WRITE_ERROR     (-2)

/* SP_UPSTREAM_ERROR - upstream error occurred (equivalent of broken pipe) */
#define SP_UPSTREAM_ERROR       (-3)

/* SP_REDUNDANT_EOF - an eof declaration (zero-sized sp_write_input()) has
 *                    already been made */
#define SP_REDUNDANT_EOF        (-4)

/* SP_MEM_ALLOC_ERROR - a memory allocation failure occurred */
#define SP_MEM_ALLOC_ERROR      (-5)

/* SP_FILE_OPEN_ERROR - a file open() error occurred */
#define SP_FILE_OPEN_ERROR      (-6)

/* SP_WRITE_ERROR - a file write error occurred */
#define SP_WRITE_ERROR          (-7)

/* SP_SORT_DEF_ERROR - an error occurred in the internal call to
 *                        nsort_define().  Check the nsort definition
 *                        string. */
#define SP_SORT_DEF_ERROR       (-8)

/* SP_SORT_EXEC_ERROR - an error occurred during sort execution. */
#define SP_SORT_EXEC_ERROR      (-9)

/* SP_GROUP_BY_MISMATCH - a call to sp_link() linked a sump pump output
 *                        where the "-match" directive for the source
 *                        sort sump pump did not match the GROUP_BY
 *                        directive for the linked to sump pump.
 *                        Either neither should be specified or both,
 *                        not just one. */
#define SP_GROUP_BY_MISMATCH   (-10)

/* SP_SORT_INCOMPATIBLE - sump pump read/write function incompatible with
                          a sort sump pump */
#define SP_SORT_INCOMPATIBLE    (-11)

/* SP_BUF_INDEX_ERROR - sump pump buffer index passed to a read/write 
                        function is invalid. */
#define SP_BUF_INDEX_ERROR      (-12)

/* SP_OUTPUT_INDEX_ERROR - sump pump output channel index passed to a 
                           read/write function is invalid. */
#define SP_OUTPUT_INDEX_ERROR   (-13)

/* SP_START_ERROR - sp_start() error */
#define SP_START_ERROR          (-14)

/* SP_SYNTAX_ERROR - error in sump pump parameter syntax */
#define SP_SYNTAX_ERROR         (-15)

/* SP_NSORT_LINK_FAILURE - link attempt to nsort library failed */
#define SP_NSORT_LINK_FAILURE   (-16)

/* SP_SORT_NOT_COMPILED - sump pump was compiled without sort support */
#define SP_SORT_NOT_COMPILED    (-17)

#define SP_PUMP_FUNCTION_ERROR (-1000)


#endif  /* !_SUMP_PUMP_H_ */
