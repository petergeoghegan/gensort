/* sump.c - SUMP Pump(TM) SMP/CMP parallel data pump library.
 *          SUMP Pump is a trademark of Ordinal Technology Corp
 *
 * $Revision: 124 $
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

#  define AIO_CAPABLE

#if !defined(_WIN32)
# define _GNU_SOURCE
# include <pthread.h>
# include <unistd.h>
# include <dlfcn.h>
# include <sched.h>
# include <sys/mman.h>
# include <sys/types.h>
# include <sys/wait.h>
# include <sys/time.h>
# include <stdint.h>
# include <ctype.h>
# include <signal.h>

# if !defined(__CYGWIN32__)
#  include <aio.h>
#  include <sys/types.h>
#  include <sys/stat.h>
# endif

# define PTFlld	"lld"
# define PTFllu	"llu"
# define PTFllx	"llx"

# define ERRNO errno

#else  /* now defined(_WIN32) */

# define PTFlld	"I64d"
# define PTFllu	"I64u"
# define PTFllx	"I64x"

# define ERRNO GetLastError()

#endif  /* !defined(_WIN32) */

#include "sump.h"
#include "sumpversion.h"
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdlib.h>
#include <string.h>

#include <string.h>
#include <errno.h>

#if defined(SUMP_PUMP_NO_SORT)
/* define some nsort typedefs to minimize the number of #if's in this file */
typedef unsigned nsort_t;       /* nsort context identifier */
typedef int nsort_msg_t;        /* return status & error message numbers */

#else
/* use Nsort include files */
# include "nsort.h"
# include "nsorterrno.h"

#endif

/* values for the flags sump pump structure member
 */
#define SP_UNICODE                      0x0001  /* not yet supported */
#define SP_UTF_8                        0x0002  /* input records are utf-8
                                                 * characters */
#define SP_ASCII   SP_UTF_8
#define SP_FIXED                        0x0003  /* input records are a fixed
                                                 * number of bytes */
#define SP_WHOLE_BUF                    0x0004  /* there are no input records,
                                                 * instead there is only an
                                                 * input buffer */
#define SP_REC_TYPE_MASK                0x0007
#define SP_SORT                         0x0008
#define SP_GROUP_BY                     0x0010
#define SP_EXEC                         0x0020

#define REC_TYPE(sp) ((sp)->flags & SP_REC_TYPE_MASK)

#define TRUE 1
#define FALSE 0

/* sort_state values */
#define SORT_INPUT      1
#define SORT_OUTPUT     2
#define SORT_DONE       3

#define ERROR_BUF_SIZE  500   /* size of error buffer */

#define DEFAULT_BUFFERED_TRANSFER_SIZE  (1024 * 1024)
#define DEFAULT_PIPE_TRANSFER_SIZE      8192


/* state structure for a sump pump instance */
struct sump
{
    unsigned            flags;         /* caller-defined bit flags,
                                        * see sump.h */
    sp_pump_t           pump_func;     /* caller-defined pump function that 
                                        * is executed in parallel by the
                                        * sump pump */
    void                *pump_arg;     /* caller-defined arg to pump func */
    unsigned            num_tasks;     /* number of pump tasks */
    unsigned            num_threads;   /* number of threads executing the
                                        * pump func */
    unsigned            num_in_bufs;   /* number of input buffers */
    unsigned            num_outputs;   /* number of sump pump output channels*/
    ssize_t             in_buf_size;   /* input buffer size in bytes */
    struct sump_out     *out;          /* array of output structures, one for
                                        * each output */
    void                *delimiter;    /* record delimiter, for text input */
    size_t              rec_size;      /* record size */
    pthread_mutex_t     sump_mtx;      /* mutex for sump pump infrastructure */
    pthread_mutex_t     sp_mtx;        /* mutex for pump funcs to use
                                        * via sp_mutex_lock() and
                                        * sp_mutex_unlock() calls */
    pthread_cond_t      in_buf_readable_cond; /* input buffer available for
                                               * reading by pump funcs */
    pthread_cond_t      in_buf_done_cond;  /* an input buffer has been
                                            * completely read by all potential
                                            * pump threads and can be reused */
    pthread_cond_t      task_avail_cond;   /* a task is available for the
                                            * taking by a sump pump thread */
    pthread_cond_t      task_drained_cond; /* a task has been completely 
                                            * executed and its output has
                                            * been drained (read) for its
                                            * output buffer(s) */
    pthread_cond_t      task_output_ready_cond; /* a task's output ready to
                                                 * be read */
    pthread_cond_t      task_output_empty_cond; /* a task's output buffer has
                                                 * been read and is empty */
    size_t              in_buf_current_bytes; /* bytes in current input buf */
    /* number of bytes at end of prev input buffer containing a partial rec */
    size_t              prev_in_buf_ending_rec_partial_bytes;
    pthread_t           *thread;        /* array of sump pump threads */
    uint64_t            cnt_in_buf_readable; /* number of input buffers that
                                              * have been filled with input
                                              * data and are available for
                                              * reading by sump pump threads
                                              * executing pump functions */
    uint64_t            cnt_in_buf_done; /* number of input buffers
                                          * that have been read by all
                                          * their readers */
    uint64_t            cnt_task_init;  /* number of tasks initialized and
                                         * available for the taking by any
                                         * sump pump thread */
    uint64_t            cnt_task_begun; /* number of tasks allocated/taken
                                         * and begun by sump pump threads */
    uint64_t            cnt_task_drained; /* number of tasks that have
                                           * been completed and had all
                                           * their output buffer(s)
                                           * completely read/drained. */
    uint64_t            cnt_task_done; /* number of done tasks whose
                                        * actual ending position has been
                                        * verified to be the same as their
                                        * expected ending */
    struct sp_task      *task;          /* array of sump pump tasks */
    struct in_buf       *in_buf;        /* array of sump pump input buffers */
    nsort_t             nsort_ctx;      /* used only if this is a sort */
    char                *error_buf;     /* buf to hold error msg */
    size_t              error_buf_size; /* buf to hold error msg */
    int                 error_code;     /* pump func generated error code */
    unsigned            sort_error;     /* sort error code */
    char                *sort_temp_buf; /* sort temporary buf */
    size_t              sort_temp_buf_size; /* size of sort temporary buf */
    size_t              sort_temp_buf_bytes; /* bytes of data in temp buf */
    char                input_eof;      /* sp_write_input() called with
                                         * size <= 0 */
    char                broken_input;   /* sp_write_input() called with
                                         * a negative size */
    char                sort_state;     /* only used for sorting */
    char                match_keys;     /* only used for sorting - indicates
                                         * -match has been specified */
    char                wait_done;      /* sp_wait already called for this sp*/
    char                in_file_alloc;  /* in_file string was malloc()'d and
                                         * should be free()'d */
    char                *in_file;       /* input file str or NULL if none */
    struct sp_file      *in_file_sp;    /* input file of sump pump */
    struct exec_state   *ex_state;      /* used when internal pump func invokes
                                         * an external executable program.
                                         * one state per sump pump thread */
    char                **exec_argv;    /* exec process command line */
};

/* struct for an output of a task */
struct task_out
{
    char        *buf;       /* output buffer where map task should
                             * write its results */
    size_t      size;       /* capacity of output buffer */
    size_t      bytes_copied; /* number of bytes written so far into
                               * output buffer */
    char        stalled;  /* the map thread handling this task is
                           * stalled waiting for the writer thread to
                           * empty its full buf */
};


#define INVALID_FD      (-1)
#define PIPE_BUF_SIZE   4096

/* macro to allow the stderr of external programs performing pump functions
 * to be the second output of the sump pump.  The initial implementation of
 * external programs for pump functions did gather stderr as the second
 * output; but it seemed problematic because when an eternal program failed
 * and wrote an error message to its standard error, the sump pump thread
 * that reads the pump func input and writes it to the standard input of
 * external program would notice that external program had terminated
 * abnormally and would set the error code for the sump pump; then the
 * thread reading the stderr of the external program would notice the sump
 * pump error code and exit before it would read the error message in the
 * stderr; thus the error message never made it through sump pump.
 *
#define SUMP_PIPE_STDERR
 */

/* per-pipe struct used when pump funcs call a separate program */
struct std_pipe
{
    struct exec_state   *ex;
    int                 perrno;
#if defined(win_nt)
    HANDLE              rd_h;   /* read handle */
    HANDLE              wr_h;   /* write handle */
#else
    int                 rd_fd;  /* read file descriptor */
    int                 wr_fd;  /* write file descriptor */
#endif
};

/* per-thread struct used when pump funcs call a separate program */
struct exec_state
{
    sp_task_t           t;
    struct std_pipe     in;
    char                in_buf[PIPE_BUF_SIZE];
    struct std_pipe     out;
    char                out_buf[PIPE_BUF_SIZE];
#if defined(SUMP_PIPE_STDERR)
    struct std_pipe     err;
    char                err_buf[PIPE_BUF_SIZE];
#endif
};

/* struct for a sump pump task */
struct sp_task
{
    struct sump *sp;            /* the "host" sp_t of this task */
    uint64_t    task_number;    /* task number */
    int         thread_index;   /* id of thread performing this task */
    char        *in_buf;        /* input buffer */
    size_t      in_buf_bytes;   /* number of bytes written into in_buf
                                 * by the reader thread.  these bytes
                                 * will be read out by map task */
    char        *rec_buf;       /* buf to hold rec returned by pf_get_rec() */
    size_t      rec_buf_size;   /* size of rec_buf */
    char        *curr_rec;      /* pointer to the current record */
    char        *temp_buf;       /* temp buf to help with printf */
    size_t      temp_buf_size;   /* size of the temp buf */
    char        *error_buf;      /* error message posted by sp_error() call */
    size_t      error_buf_size;  /* size of the error message buf */
    int         error_code;      /* pump func generated error code */
    int         sort_error;      /* nsort error code */
    uint64_t    curr_in_buf_index; /* current input buffer index */
    char        *begin_rec;      /* pointer to the beginning record */
    uint64_t    begin_in_buf_index;/* beginning input buffer index */
    /* the following 2 members are set by the thread calling sp_write_input()
     */
    uint64_t    expected_end_index; /* expected end in buf index */
    int         expected_end_offset; /* expected end in buf offset */
    
    char        first_group_rec; /* next record read will be the first
                                  * record for its record group */
    char        first_in_buf;     /* this task is still reading its
                                   * first input buffer */
    char        input_eof;       /* boolean: this task is done reading
                                  * its input */
    char        output_eof;      /* boolean: thread performing task is
                                  * done writing its output to its output
                                  * buffer, but we may still need to wait
                                  * until the output buffer has been read
                                  * before the task is done */
    int         outs_drained;   /* number of outputs for this task that
                                 * have been completely drained (read) */
    struct task_out *out;       /* array of task outputs */
};

/* struct for a sump pump input buffer */
typedef struct in_buf
{
    char        *in_buf;        /* input buffer */
    size_t      in_buf_bytes;   /* number of bytes written into in_buf
                                 * by the reader thread.  these bytes
                                 * will be read out by map task */
    size_t      in_buf_size;    /* size of the in_buf */
    size_t      alloc_size;     /* allocation size of the in_buf */
    unsigned    num_readers;    /* number of threads performing
                                 * tasks that read this buf */
    unsigned    num_readers_done;/* number of reader threads that are
                                  * done with this buffer */
} in_buf_t;

/* struct for a link (copy thread) between an output of one sump pump and
 * the input of another.
 */
struct sp_link
{
    struct sump         *out_sp;        /* sp_t we are reading from */
    unsigned            out_index;      /* output index of read sp_t */
    struct sump         *in_sp;         /* sp_t we are writing to */
    size_t              buf_size;       /* buf size */
    char                *buf;           /* temp buf for transfering data */
    pthread_t           thread;         /* thread executing link_main() */
    int                 error_code;     /* error code */
};

/* struct for a file reader or writer thread */
struct sp_file
{
    char        *fname;         /* file name */
    pthread_t   thread;         /* thread executing either file_reader() or
                                 * file_writer() */
    sp_t        sp;             /* sump pump this file */
    int         mode;           /* file access mode */
#if defined(win_nt)
    HANDLE      fd;             /* file handle */
#else
    int         fd;             /* file descriptor */
#endif
    char        wait_done;      /* sp_wait already called for this sp_file */
    int         out_index;      /* sump pump output index (if relevant) */
    int         aio_count;      /* the max and target number of async i/o's */
    size_t      transfer_size;  /* read or write request size */
    int         error_code;     /* error code */
    int         can_seek;       /* if true, then direct/async-capable file */
    int         is_std;         /* file is either stdin, stdout or stderr */
};

/* file access modes */
#define MODE_UNSPECIFIED    0   /* no access mode has been specified */
#define MODE_BUFFERED       1   /* use standard read() or write() calls */
#define MODE_DIRECT         2   /* direct and asynchronous r/w requests */


/* struct for a sump pump output */
struct sump_out
{
    size_t              buf_size;      /* size of each task's corresponding
                                        * output buffer for this output */
    int                 size_specified; /* boolean: non-zero if out buf size
                                         * has been set */
    double              buf_size_mult;/* factor increase over input buf size */
    size_t              partial_bytes_copied; /* the number of bytes copied
                                               * from the task output buffer
                                               * currently being read from */
    char                file_alloc;     /* file name was malloc()'d and
                                         * should be free()'d */
    char                *file;         /* output file str or NULL if none */
    struct sp_file      *file_sp;      /* output file for this sump pump out */
    uint64_t            cnt_task_drained; /* number of tasks that have
                                           * been completed and their
                                           * output buffer for this
                                           * particular output has been
                                           * completely read */
};


#if defined(AIO_CAPABLE)
/* sump aio struct */ 
struct sump_aio
{
    uint64_t            buf_index;      /* sump pump buffer index */
    size_t              buf_offset;     /* beginning io offset within buffer */
    char                last_buf_io;    /* boolean indicating last io for buf*/
    int64_t             file_offset;    /* file offset */
    size_t              nbytes;         /* request size */
    struct aiocb        aio;
};
#endif


/* global sump pump mutex */
static pthread_mutex_t  Global_lock = PTHREAD_MUTEX_INITIALIZER;

static sp_t             Global_external_sp;
static int              Global_external_count;

/* file descriptor for /dev/zero */
static int Zero_fd;

/* default size for sp_write_input() and sp_read_output() transfers for
 * regression testing of those interfaces.
 */
static size_t Default_rw_test_size;

/* default file access mode */
static int Default_file_mode = MODE_DIRECT;


/* die - quit program due to a fatal sump pump infrastructure error.
 */
static void die(char *fmt, ...)
{
    va_list     ap;

    fprintf(stderr, "sump pump fatal error: ");
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
    exit(1);
}


#define PAGE_SIZE       page_size()

/* page_size - internal routine to get the system page size.
 */
static int page_size()
{
    static int  size;

    if (size == 0)
    {
#if defined(win_nt)
        SYSTEM_INFO si;
    
        GetSystemInfo(&si);
        size = si.dwPageSize;
#else
        size = getpagesize();
#endif
    }
    return (size);
}


/* sp_get_time_us  - return elapsed time in microseconds.
 *
 *      The caller can discard the upper 32 bits *only* when timing intervals
 *      less than ~4000 seconds = 1 hour 11 minutes.
 */
static uint64_t sp_get_time_us(void)
{
    struct timeval      time;
    static uint64_t     begin;

    if (begin == 0)  /* if first time */
    {
        if (gettimeofday(&time, NULL) < 0)
            die("gettimeofday() failure\n");
        begin = (time.tv_sec * (uint64_t)1000 * 1000) + time.tv_usec;
    }
    if (gettimeofday(&time, NULL) < 0)
        die("gettimeofday() failure\n");
    return (time.tv_sec * (uint64_t)1000 * 1000) + time.tv_usec - begin;
}


static FILE    *TraceFp;
#define TRACE if (TraceFp != NULL) trace

/* trace - print a trace message
 */
static void trace(const char *fmt, ...)
{
    va_list     ap;
    uint64_t    diff = sp_get_time_us();
    int	        seconds = (int)(diff / 1000000);
    int         fractions = (int)(diff % 1000000);

    fprintf(TraceFp, "%2d.%06d: ", seconds, fractions);
    
    va_start(ap, fmt);
    vfprintf(TraceFp, fmt, ap);
    va_end(ap);
    fflush(TraceFp);
}


/* sp_get_version - get the subversion version for sump pump
 *
 * Returns: a string containing the subversion version. The string should
 *          NOT be free()'d.
 */
const char *sp_get_version(void)
{
    return (sp_version);
}


/* sp_get_id - get the subversion id keyword substitution for sump pump
 *
 * Returns: a string containing the subversion id keyword. The string should
 *          NOT be free()'d.
 */
const char *sp_get_id(void)
{
    return ("$Id$");
}


/* get_error_msg - place a system error message in the provided buffer
 */
static char *get_error_msg(int error, char *err_buf, size_t err_buf_size)
{
#if defined(win_nt)
    char		*lpMsgBuf;
    char		*eol;

    if (error == 0)  /* if no error specified, get last error */
        error = GetLastError();
    FormatMessage(FORMAT_MESSAGE_ALLOCATE_BUFFER | 
                  FORMAT_MESSAGE_FROM_SYSTEM | 
                  FORMAT_MESSAGE_IGNORE_INSERTS,
                  GetModuleHandle(NULL),
                  error,
                  MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
                  (LPTSTR) &lpMsgBuf,
                  0,
                  NULL);
    eol = lpMsgBuf + strlen(lpMsgBuf);
    /* remove trailing \r\n so (%d) appears on the same line */
    if (eol > lpMsgBuf && eol[-1] == '\n' && eol[-2] == '\r')
        eol[-2] = '\0';
	
    _snprintf(err_buf, err_buf_size - 1, "%s (%d)", lpMsgBuf, error);
    LocalFree(lpMsgBuf);
    return (err_buf);
#else
    if (error == 0)  /* if no error specified, get last error */
        error = errno;
    return (strerror_r(error, err_buf, err_buf_size));
#endif    
}


/* start_error - raise an error and set error message during sp_start()
 *               but before the sump pump threads are created.
 */
static int start_error(sp_t sp, const char *fmt, ...)
{
    va_list     ap;
    int         ret;

    if (sp->error_code != 0)          /* if prior error */
        return sp->error_code;        /* ignore this one */
    sp->error_code = SP_START_ERROR;

    va_start(ap, fmt);
    ret = vsnprintf(sp->error_buf, sp->error_buf_size, fmt, ap);
    va_end(ap);
#if defined(win_nt)
    if (ret == -1)  /* non-standard vsnprintf overflow indicator on Windows */
    {
        va_start(ap, fmt);
        ret = _vscprintf(fmt, ap);
        va_end(ap);
    }
#endif
    if ((size_t)ret >= sp->error_buf_size)
    {
        if (sp->error_buf_size != 0)
            free(sp->error_buf);
        sp->error_buf_size = (size_t)ret + 1;
        sp->error_buf = (char *)malloc(sp->error_buf_size);
        va_start(ap, fmt);
        vsnprintf(sp->error_buf, sp->error_buf_size, fmt, ap);
        va_end(ap);
    }
    
    return SP_START_ERROR;
}


/* broadcast_all_conds - internal routine to broadcast all sump pump conditions
 *                       The sump_mtx should already be locked.
 */
static void broadcast_all_conds(sp_t sp)
{
    pthread_cond_broadcast(&sp->in_buf_readable_cond); /*multiple sp threads*/
    pthread_cond_broadcast(&sp->in_buf_done_cond);/* sp_write_input() caller */
    pthread_cond_broadcast(&sp->task_avail_cond);   /* multiple sp threads */
    pthread_cond_broadcast(&sp->task_drained_cond);/* sp_write_input() caller*/
    pthread_cond_broadcast(&sp->task_output_ready_cond); /* mult sp threads */
    pthread_cond_broadcast(&sp->task_output_empty_cond); /* mult sp threads */
}


/* sp_raise_error - raise an error for a sump pump
 */
void sp_raise_error(sp_t sp, int error_code, const char *fmt, ...)
{
    va_list     ap;
    int         ret;

    pthread_mutex_lock(&sp->sump_mtx);
    if (sp->error_code != 0)            /* if prior error */
    {
        pthread_mutex_unlock(&sp->sump_mtx);
        return;                /* ignore this one. let prior error stand */
    }
    sp->error_code = error_code;
    
    va_start(ap, fmt);
    ret = vsnprintf(sp->error_buf, sp->error_buf_size, fmt, ap);
    va_end(ap);
#if defined(win_nt)
    if (ret == -1)  /* non-standard vsnprintf overflow indicator on Windows */
    {
        va_start(ap, fmt);
        ret = _vscprintf(fmt, ap);
        va_end(ap);
    }
#endif
    if ((size_t)ret >= sp->error_buf_size)
    {
        if (sp->error_buf_size != 0)
            free(sp->error_buf);
        sp->error_buf_size = (size_t)ret + 1;
        sp->error_buf = (char *)malloc(sp->error_buf_size);
        va_start(ap, fmt);
        vsnprintf(sp->error_buf, sp->error_buf_size, fmt, ap);
        va_end(ap);
    }
    /* Wake up all possible waiting threads for the sump pump.
     * Some of the below pthread_cond_broadcasts could just be signals.
     * But since error handling isn't a performance critical operation,
     * signals are used instead.
     */
    broadcast_all_conds(sp);
    pthread_mutex_unlock(&sp->sump_mtx);
}


#if defined(win_nt)

# include "sump_win.c"

#else

/* init_zero_fd - initialize Zero_fd, the file descriptor for /dev/zero.
 */
static void init_zero_fd()
{
    pthread_mutex_lock(&Global_lock);
    if (Zero_fd <= 0)
        Zero_fd = open("/dev/zero", O_RDWR);
    pthread_mutex_unlock(&Global_lock);
    return;
}

#endif


#if !defined(SUMP_PUMP_NO_SORT)

/* function pointers to nsort library entry points.  These are 
 * non-NULL if the nsort library is linked in.
 */
nsort_msg_t (*Nsort_define)(const char *def,
                            unsigned options,
                            nsort_error_callback_t *callbacks,
                            nsort_t *ctxp);
nsort_msg_t (*Nsort_release_recs)(void *buf, 
                                  size_t size, 
                                  nsort_t *ctxp);
nsort_msg_t (*Nsort_release_end)(nsort_t *ctxp);
nsort_msg_t (*Nsort_return_recs)(void *buf,
                                 size_t *size, 
                                 nsort_t *ctxp);
nsort_msg_t (*Nsort_end)(nsort_t *ctxp);
const char *(*Nsort_get_stats)(nsort_t *ctxp);
char *(*Nsort_message)(nsort_t *ctxp);
char *(*Nsort_version)(void);

typedef nsort_msg_t (*declare_function_t)(char *name, nsort_compare_t func, void *arg);
typedef nsort_msg_t (*define_t)(const char *, unsigned, nsort_callback_t *, nsort_t *ctxp);
typedef nsort_msg_t (*merge_define_t)(const char *def, unsigned options, nsort_error_callback_t *callbacks, int merge_width, nsort_merge_callback_t *merge_input, nsort_t *ctxp);
typedef nsort_msg_t (*release_recs_t)(void *buf, size_t size, nsort_t *ctxp);
typedef nsort_msg_t (*release_end_t)(nsort_t *ctxp);
typedef nsort_msg_t (*return_recs_t)(void *buf, size_t *size, nsort_t *ctxp);
typedef nsort_msg_t (*print_stats_t)(nsort_t *ctxp, FILE *fp);
typedef const char  *(*get_stats_t)(nsort_t *ctxp);
typedef char        *(*message_t)(nsort_t *ctxp);
typedef nsort_msg_t (*end_t)(nsort_t *ctxp);
typedef char        *(*version_t)(void);


/* get_nsort_syms - internal routine to dynamically link to nsort library
 */
static int get_nsort_syms()
{
# if defined(win_nt)
#  define dlsym(handle, name)   GetProcAddress((handle), (name))
    HANDLE      syms;

    if ((syms = LoadLibrary("libnsort.dll")) == NULL)
        return (-1);
# else
    void        *syms;

    if ((syms = dlopen("libnsort.so", RTLD_GLOBAL | RTLD_LAZY)) == NULL)
        return (-1);
# endif
    if ((Nsort_define = (define_t)dlsym(syms, "nsort_define")) == NULL)
        return (-2);
    if ((Nsort_release_recs = (release_recs_t)dlsym(syms, "nsort_release_recs")) == NULL)
        return (-2);
    if ((Nsort_release_end = (release_end_t)dlsym(syms, "nsort_release_end")) == NULL)
        return (-2);
    if ((Nsort_return_recs = (return_recs_t)dlsym(syms, "nsort_return_recs")) == NULL)
        return (-2);
    if ((Nsort_end = (end_t)dlsym(syms, "nsort_end")) == NULL)
        return (-2);
    if ((Nsort_get_stats = (get_stats_t)dlsym(syms, "nsort_get_stats")) == NULL)
        return (-2);
    if ((Nsort_message = (message_t)dlsym(syms, "nsort_message")) == NULL)
        return (-2);
    if ((Nsort_version = (version_t)dlsym(syms, "nsort_version")) == NULL)
        return (-2);
    return (0);
}


/* link_in_nsort - internal routine to, if not already done, dynamically
 *                 link in the nsort library.
 */
static int link_in_nsort()
{
    int ret;
    
# if !defined(win_nt)
    pthread_mutex_lock(&Global_lock);
# endif
    ret = Nsort_define == NULL ? get_nsort_syms() : 0;
# if !defined(win_nt)
    pthread_mutex_unlock(&Global_lock);
# endif
    return (ret);
}    


/* post_nsort_error - internal routine to post an error received from nsort.
 */
static void post_nsort_error(sp_t sp, unsigned ret)
{
    pthread_mutex_lock(&sp->sump_mtx);
    if (sp->error_code == SP_OK)  /* if no other error yet, this is the one */
    {
        char *msg = (*Nsort_message)(&sp->nsort_ctx);
        
        sp->error_code = SP_SORT_EXEC_ERROR;
        sp->sort_error = ret;
        if (msg == NULL)
            msg = "No Nsort error message";
        strncpy(sp->error_buf, msg, sp->error_buf_size);
        sp->error_buf[sp->error_buf_size - 1] = '\0';  /* handle overflow */
    }
    sp->sort_error = ret;
    sp->sort_state = SORT_DONE;
    pthread_cond_broadcast(&sp->task_output_ready_cond);
    pthread_mutex_unlock(&sp->sump_mtx);
}


#define STAT_DRCTV      " -stat"


/* sp_start_sort - start an nsort instance with a sump pump wrapper so
 *                 that its input or output can be assigned to a file or
 *                 linked to another sump pump.  For instance, the sort
 *                 input or output can be linked to sump pump performing
 *                 record pumping such as "map" on input and "reduce"
 *                 for output.
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
int sp_start_sort(sp_t *caller_sp,
                  char *def_fmt,
                  ...)
{
    int                 ret;
    char                *def_plus = NULL;
    sp_t                sp;
    size_t              def_len;
    char                *def;
    char                thread_drctv[30];
    unsigned char       *p;
# if defined(win_nt)
    SYSTEM_INFO         si;
# endif

    *caller_sp = NULL;  /* assume the worst for now */
    sp = (sp_t)calloc(1, sizeof(struct sump));
    if (sp == NULL)
        return (SP_MEM_ALLOC_ERROR);
    sp->error_buf_size = ERROR_BUF_SIZE;
    sp->error_buf = (char *)calloc(1, sp->error_buf_size);
    if (sp->error_buf == NULL)
        return (SP_MEM_ALLOC_ERROR);
    *caller_sp = sp;  /* allow access to error_buf even if failure */
    /* fill in default parameters */
# if defined(win_nt)
    GetSystemInfo(&si);
    sp->num_threads = si.dwNumberOfProcessors;
# else
    sp->num_threads = sysconf(_SC_NPROCESSORS_ONLN);
# endif
    sprintf(thread_drctv, "-threads=%d ", sp->num_threads);
    if (sp->num_outputs > 32)
        sp->num_outputs = 32;
    sp->num_outputs = 1;
    sp->out = (struct sump_out *)calloc(1, sizeof(struct sump_out));
    sp->delimiter = (void *)"\n";
    sp->rec_size = 0;

    if (link_in_nsort() != 0)    /* if error */
    {
        sp->error_code = SP_NSORT_LINK_FAILURE;
        sp->sort_state = SORT_DONE;
        return (sp->error_code);
    }
    
    sp->flags |= SP_SORT;

    if (def_fmt != NULL)
    {
        va_list ap;
        size_t  def_len;
        
        va_start(ap, def_fmt);
#if defined(win_nt)
        def_len = _vscprintf(def_fmt, ap);
#else
        def_len = vsnprintf(NULL, 0, def_fmt, ap);
#endif
        va_end(ap);
        def = (char *)calloc(def_len + 1, 1);
        va_start(ap, def_fmt);
        if (vsnprintf(def, def_len + 1, def_fmt, ap) != def_len)
            die("sp_start_sort: vnsprintf failed to return %d\n", def_len);
        va_end(ap);
    }
    else
        def = "";

    def_len = strlen(def) + 1;  /* plus '\0' */
    def_len += strlen(STAT_DRCTV);
    def_len += strlen(thread_drctv);
    def_plus = (char *)malloc(def_len);
    if (def_plus == NULL)
        return (SP_MEM_ALLOC_ERROR);
    strcpy(def_plus, thread_drctv); /* goes first so caller can override */
    strcat(def_plus, def);
    strcat(def_plus, STAT_DRCTV);

    /* check for input file declaration */
    for (p = (unsigned char *)def_plus; *p != '\0'; p++)
    {
        if (p[0] == '-' || p[0] == '/')
        {
            p++;
            /* detect -IN[_]F[ILE] */
            if (toupper(p[0]) == 'I' && toupper(p[1]) == 'N' &&
                (toupper(p[2]) == 'F' || p[2] == '_'))
            {
                sp->in_file = "<defined to nsort>";
                p += 3;
            }
            /* detect -OUT[_]F[ILE] */
            else if (toupper(p[0]) == 'O' && toupper(p[1]) == 'U' &&
                     toupper(p[2]) == 'T' &&
                     (toupper(p[3]) == 'F' || p[3] == '_'))
            {
                sp->out[0].file = "<defined to nsort>";
                p += 4;
            }
            /* detect -MATCH */
            else if (toupper(p[0]) == 'M' && toupper(p[1]) == 'A' &&
                     toupper(p[2]) == 'T' && toupper(p[3]) == 'C' &&
                     toupper(p[4]) == 'H')
            {
                sp->match_keys = TRUE;
                p += 5;
            }
        }
    }

    /* check for output file declaration */
    
    ret = (*Nsort_define)(def_plus, 0, NULL, &sp->nsort_ctx);
    free(def_plus);
    if (def_fmt != NULL)
        free(def);
    if (ret < 0)
    {
        char *msg = (*Nsort_message)(&sp->nsort_ctx);
        
        sp->sort_error = ret;
        sp->error_code = SP_SORT_DEF_ERROR;
        if (msg == NULL)
            msg = "No Nsort error message";
        strncpy(sp->error_buf, msg, sp->error_buf_size - 1);
        sp->error_buf[sp->error_buf_size - 1] = '\0';  /* handle overflow */
        sp->sort_state = SORT_DONE;
        return (SP_SORT_DEF_ERROR);
    }
    sp->sort_state = sp->in_file != NULL ? SORT_OUTPUT : SORT_INPUT;
    pthread_mutex_init(&sp->sump_mtx, NULL);
    /* use output ready cond for state changes */
    pthread_cond_init(&sp->task_output_ready_cond, NULL);
    sp->sort_temp_buf_size = sp->out[0].buf_size ? sp->out[0].buf_size : 4096;
    if ((sp->sort_temp_buf = (char *)malloc(sp->sort_temp_buf_size)) == NULL)
        return (SP_MEM_ALLOC_ERROR);
    return (SP_OK);
}


/* sp_get_sort_stats - get a string containing the nsort statistics report
 *                     for an nsort sump pump that has completed.
 *
 * Returns: a string containing the Nsort statistics report. The string should
 *          NOT be free()'d and is valid until the passed sp_t is sp_free()'d.
 */
const char *sp_get_sort_stats(sp_t sp)
{
    const char  *ret;
    
    if (sp->error_code)
        return ("no stats because of nsort error");

    if (!(sp->flags & SP_SORT))
        return (NULL);
    if ((ret = (*Nsort_get_stats)(&sp->nsort_ctx)) == NULL)
        ret = "Nsort_get_stats() failure\n";
    return (ret);
}


/* sp_get_nsort_version - get the subversion version for sump pump
 *
 * Returns: a string containing the Nsort version number. The string should
 *          NOT be free()'d.
 */
const char *sp_get_nsort_version(void)
{
    if (link_in_nsort() != 0)    /* if error */
    {
        return ("Nsort could not be linked in");
    }
    return ((*Nsort_version)());
}

#else

/* sp_start_sort - dummy non-sort version.
 */
int sp_start_sort(sp_t *caller_sp,
                  char *def_fmt,
                  ...)
{
    *caller_sp = NULL;
    return (SP_SORT_NOT_COMPILED);
}


/* sp_get_sort_stats - dummy non-sort version.
 */
const char *sp_get_sort_stats(sp_t sp)
{
    return ("");
}


/* sp_get_nsort_version - dummy non-sort version.
 */
const char *sp_get_nsort_version(void)
{
    return ("this SUMP Pump version has not been compiled for nsort");
}


#endif /* !defined(SUMP_PUMP_NO_SORT) */

/* pipe_reader - thread start function for thread that reads either the
 *               stdout or stderr of an external process, writes the output
 *               to either task output 0 or 1, respectively.
 */
void *pipe_reader(void *arg)
{
    struct sump         *sp;
    struct exec_state   *ex;
    struct std_pipe     *pipe;
    sp_task_t           t;
    char                *buf;
    int                 out_index;
    ssize_t             len;
    ssize_t             total = 0;
    ssize_t             buf_size;
    ssize_t             buf_bytes;
    int                 ret;

    pipe = (struct std_pipe *)arg;
    ex = pipe->ex;
    t = ex->t;
    sp = t->sp;

#if defined(SUMP_PIPE_STDERR)
    /* determine if we are reading stdout or stderr */
    if (pipe == &ex->err)
    {
        buf = ex->err_buf;
        out_index = 1;
    }
    else
#endif
    {
        buf = ex->out_buf;
        out_index = 0;
    }
    TRACE("pipe_reader%d: started\n", t->thread_index);

    /* if we are just writing to a whole input buffer.
     */
    if (sp->flags & SP_WHOLE_BUF)
    {
        /* get output buffer */
        ret = pfunc_get_out_buf(t, out_index,
                                (void **)&buf, (size_t *)&buf_size);
        if (ret != 0)
        {
            pfunc_error(t, "internal error: "
                        "bad ret from sp_get_out_buf: %d\n", ret);
            goto reader_return;
        }
        
        /* read from pipe into output buffer, flushing buffer when necessary */
        buf_bytes = 0;
        for (;;)
        {
#if defined(win_nt)
            DWORD       size;
            
            if (ReadFile(pipe->rd_h, buf + buf_bytes,
                         buf_size - buf_bytes, &size, NULL))
                len = size;     /* success */
            else if (GetLastError() == ERROR_BROKEN_PIPE)
                len = 0;        /* eof */
            else
                len = -1;       /* failure */
#else
            len = read(pipe->rd_fd, buf + buf_bytes, buf_size - buf_bytes);
#endif
            if (len == 0)
                break;
            if (len < 0)
            {
                pipe->perrno = ERRNO;
                buf_bytes = 0;
                break;
            }
            buf_bytes += len;
            if (buf_bytes == buf_size)
            {
                /* commit/flush output bytes */
                ret = pfunc_put_out_buf_bytes(t, out_index, buf_bytes);
                if (ret != 0)
                {
                    pfunc_error(t, "internal error: " 
                                "bad ret from sp_put_out_buf_bytes: %d\n",ret);
                    goto reader_return;
                }
                buf_bytes = 0;
            }
        }
        if (buf_bytes != 0)
        {
            /* commit/flush output bytes */
            ret = pfunc_put_out_buf_bytes(t, out_index, buf_bytes);
            if (ret != 0)
            {
                pfunc_error(t, "internal error: " 
                            "bad ret from sp_put_out_buf_bytes: %d\n",ret);
                goto reader_return;
            }
        }
    }
    else
    {
        /* read from pipe and write to task output */
        for (;;)
        {
#if defined(win_nt)
            DWORD       size;
            
            if (ReadFile(pipe->rd_h, buf, PIPE_BUF_SIZE, &size, NULL))
                len = size;     /* success */
            else if (GetLastError() == ERROR_BROKEN_PIPE)
                len = 0;        /* eof */
            else
                len = -1;       /* failure */
#else
            len = read(pipe->rd_fd, buf, PIPE_BUF_SIZE);
#endif
            TRACE("pipe_reader%d: read %d bytes\n", t->thread_index, (int)len);
            if (len == 0)
                break;
            if (len < 0)
            {
                pipe->perrno = ERRNO;
                break;
            }
            total += len;
            if (pfunc_write(t, out_index, buf, len) != len)
            {
                pfunc_error(t, "internal error: pfunc_write failure\n");
                break;
            }
        }
    }
  reader_return:
#if defined(win_nt)
    CloseHandle(pipe->rd_h);
#else
    pfunc_mutex_lock(t);
    {
        close(pipe->rd_fd);
        pipe->rd_fd = INVALID_FD;
    }
    pfunc_mutex_unlock(t);
#endif
    TRACE("pipe_reader%d: returning\n", t->thread_index);

    return (NULL);
}

/* pfunc_exec - internal pump function to pipe the task input to an external
 *              process, and read back the process stdout and stderr and
 *              write to task outputs 0 and 1 respectively.
 */
int pfunc_exec(sp_task_t t, void *unused)
{
    char                *rec;
    struct exec_state   *ex;
    int                 ti = pfunc_get_thread_index(t);
    struct sump         *sp;
    pthread_t           out_thread;
#if defined(SUMP_PIPE_STDERR)
    pthread_t           err_thread;
#endif
    int                 buf_bytes;
    pid_t               child;
    int                 status;
    char	        errmsg[100];
    /* just_input_files is a half-finished implementation using temporary files
     * rather than pipes to transport input data to the external program */
    int                 just_input_files = 0;
#if defined(win_nt)
    DWORD               wr_size;
    PROCESS_INFORMATION pi;
#else
    int                 tempfds[2];
#endif

    sp = t->sp;
    ex = &sp->ex_state[ti];
    ex->t = t;
    ex->in.perrno = 0;
    ex->out.perrno = 0;
#if defined(SUMP_PIPE_STDERR)
    ex->err.perrno = 0;
#endif

    if (!just_input_files)
    {
#if defined(win_nt)
        SECURITY_ATTRIBUTES     sa;
        STARTUPINFOA            sui;
        
        sa.nLength = sizeof(SECURITY_ATTRIBUTES);
        sa.bInheritHandle = TRUE;
        sa.lpSecurityDescriptor = NULL;

        /* create pipe for stdin of external process */
        if (!CreatePipe(&ex->in.rd_h, &ex->in.wr_h, &sa, 0))
            return (pfunc_error(t, "CreatePipe() error: %s\n",
                                get_error_msg(0, errmsg, sizeof(errmsg))));
        /* make sure the write handle is not inherited */
        if (!SetHandleInformation(ex->in.wr_h, HANDLE_FLAG_INHERIT, 0))
            return (pfunc_error(t, "SetHandleInformation() error: %s\n",
                                get_error_msg(0, errmsg, sizeof(errmsg))));

        /* create pipe for stdout of external process */
        if (!CreatePipe(&ex->out.rd_h, &ex->out.wr_h, &sa, 0))
            return (pfunc_error(t, "CreatePipe() error: %s\n",
                                get_error_msg(0, errmsg, sizeof(errmsg))));
        /* make sure the read handle is not inherited */
        if (!SetHandleInformation(ex->out.rd_h, HANDLE_FLAG_INHERIT, 0))
            return (pfunc_error(t, "SetHandleInformation() error: %s\n",
                                get_error_msg(0, errmsg, sizeof(errmsg))));

        ZeroMemory(&sui, sizeof(STARTUPINFOA));
        sui.cb = sizeof(STARTUPINFO);
        sui.hStdInput = ex->in.rd_h;
        sui.hStdOutput = ex->out.wr_h;
        sui.hStdError = GetStdHandle(STD_ERROR_HANDLE); /* use own handle */
        sui.dwFlags |= STARTF_USESTDHANDLES;

        ZeroMemory(&pi, sizeof(PROCESS_INFORMATION));

        if (!CreateProcessA(NULL, /* NULL process name forces use of cmd line
                                   * where the PATH may be used to find exe */
                            sp->exec_argv[0],/* command line */
                            NULL,            /* process security attributes */
                            NULL,            /* thread security attributes */
                            TRUE,            /* handles are inherited */
                            0,               /* flags */
                            NULL,            /* environment */
                            NULL,            /* current directory */
                            &sui,            /* startup info */
                            &pi))            /* process info */
        {
            return (pfunc_error(t, "CreateProcess() error: %s\n",
                                get_error_msg(0, errmsg, sizeof(errmsg))));
        }
        CloseHandle(pi.hThread);

        /* close the original write handle for external proc's stdout */
        if (!CloseHandle(ex->out.wr_h))
        {
            return (pfunc_error(t, "pipe_reader, CloseHandle failure: %d\n", 
                                GetLastError()));
        }
#else
        /* lock out other pump functions (even in other sump pumps)
         * and their fd opens and closes
         */
        pthread_mutex_lock(&Global_lock);
        if (Global_external_count == 0)
            Global_external_sp = sp;
        else if (Global_external_sp != sp)
        {
            /* there is more than one sump pump simultaneously active
             * invoking an external process. This code is not set up to
             * properly close the write fds (to the stdins of its external
             * processes) for the other sump pump.  For now, declare error
             * and return.
             */
            pthread_mutex_unlock(&Global_lock);
            return (pfunc_error(t, "more than one sump active with external processes\n"));
        }
        /* use curly braces to better illustrate mutex block */
        {       
            if (pipe(tempfds) != 0)
            {
                pthread_mutex_unlock(&Global_lock);
                return (pfunc_error(t, "pipe() error: %s\n", strerror(errno)));
            }
            ex->in.rd_fd = tempfds[0];
            ex->in.wr_fd = tempfds[1];
            if (pipe(tempfds) != 0)
            {
                pthread_mutex_unlock(&Global_lock);
                return (pfunc_error(t, "pipe() error: %s\n", strerror(errno)));
            }
            ex->out.rd_fd = tempfds[0];
            ex->out.wr_fd = tempfds[1];
# if defined(SUMP_PIPE_STDERR)
            if (pipe(tempfds) != 0)
            {
                pthread_mutex_unlock(&Global_lock);
                return (pfunc_error(t, "pipe() error: %s\n", strerror(errno)));
            }
            ex->err.rd_fd = tempfds[0];
            ex->err.wr_fd = tempfds[1];
# endif
            child = fork();
            if (child == -1)
            {
                pthread_mutex_unlock(&Global_lock);
                return (pfunc_error(t, "fork() error: %s\n", strerror(errno)));
            }
            else if (child == 0)
            {
                /* current process is the newly created child */
                int         i;

                if (dup2(ex->in.rd_fd, STDIN_FILENO) == -1)
                {
                    perror("dup2() for stdin");
                    exit(1);
                }
                close(ex->in.rd_fd);
                ex->in.rd_fd = INVALID_FD;

                if (dup2(ex->out.wr_fd, STDOUT_FILENO) == -1)
                {
                    perror("dup2() for stdout");
                    exit(1);
                }
                close(ex->out.wr_fd);
                ex->out.wr_fd = INVALID_FD;
# if defined(SUMP_PIPE_STDERR)
                if (dup2(ex->err.wr_fd, STDERR_FILENO) == -1)
                {
                    perror("dup2() for stderr");
                    exit(1);
                }
                close(ex->err.wr_fd);
                ex->err.wr_fd = INVALID_FD;
# endif
                /* close all sump-pump-end pipe file descriptors.
                 * presumably closing these red
                 */
                for (i = 0; i < sp->num_threads; i++)
                {
                    if (sp->ex_state[i].in.wr_fd != INVALID_FD)
                        close(sp->ex_state[i].in.wr_fd);
                    if (sp->ex_state[i].out.rd_fd != INVALID_FD)
                        close(sp->ex_state[i].out.rd_fd);
# if defined(SUMP_PIPE_STDERR)
                    if (sp->ex_state[i].err.rd_fd != INVALID_FD)
                        close(sp->ex_state[i].err.rd_fd);
# endif
                }

                /* exec program */
                execvp(sp->exec_argv[0], sp->exec_argv);

                /* if this path is reached, then the previous exec failed */
                exit(errno);
            }
        
            /* this is the pfunc thread (and not the child process) */
            /* close the child's ends of the 3 pipes */
            close(ex->in.rd_fd);
            ex->in.rd_fd = INVALID_FD;

            close(ex->out.wr_fd);
            ex->out.wr_fd = INVALID_FD;
# if defined(SUMP_PIPE_STDERR)
            close(ex->err.wr_fd);
            ex->err.wr_fd = INVALID_FD;
# endif
        }    
        Global_external_count++;
        pthread_mutex_unlock(&Global_lock);
#endif

        /* create thread for each of stdout and stderr */
        if (pthread_create(&out_thread, NULL, pipe_reader, &ex->out) != 0)
            return (pfunc_error(t, "pthread_create out error: %d\n", ERRNO));
#if defined(SUMP_PIPE_STDERR)
        if (pthread_create(&err_thread, NULL, pipe_reader, &ex->err) != 0)
            return (pfunc_error(t, "pthread_create err error: %d\n", ERRNO));
#endif
    }
    else
    {
#if defined(win_nt)
#else
        char    fname[50];
        
        sprintf(fname, "sptaskinput%llu",
                (long long unsigned int)t->task_number);
        ex->in.wr_fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0777);
        if (ex->in.wr_fd < 0)
        {
            strerror_r(errno, errmsg, sizeof(errmsg));
            return(pfunc_error(t,
                               "can't open debug input file '%s': %s\n",
                               fname, errmsg));
        }
#endif
    }

    /* if we are just reading a whole input buffer, not record by record.
     */
    if (sp->flags & SP_WHOLE_BUF)
    {
        unsigned char   *in;
        size_t          in_size;
        int             ret;

        /* get input buffer */
        if ((ret = pfunc_get_in_buf(t, (void **)&in, &in_size)) != 0)
            return (pfunc_error(t, "internal pfunc error: "
                                "bad ret from sp_get_in_buf: %d\n", ret));
#if defined(win_nt)
        if (!WriteFile(ex->in.wr_h, in, in_size, &wr_size, NULL))
#else
        if (write(ex->in.wr_fd, in, in_size) != in_size)
#endif
            ex->in.perrno = ERRNO;
    }
    else  /* the records are lines of text */
    {
        buf_bytes = 0;
        for (;;)
        {
            /* for each record in the group */
            while (pfunc_get_rec(t, &rec) > 0)
            {
                int     len;
                int     copy_size;
        
                /* buffer and write to in.wr_fd */
                len = strlen(rec);
                while (len)
                {
                    copy_size = len;
                    if (copy_size > PIPE_BUF_SIZE - buf_bytes)
                        copy_size = PIPE_BUF_SIZE - buf_bytes;
                    memcpy(ex->in_buf + buf_bytes, rec, copy_size);
                    buf_bytes += copy_size;
                    if (buf_bytes == PIPE_BUF_SIZE)
                    {
#if defined(win_nt)
                        if (!WriteFile(ex->in.wr_h, ex->in_buf, buf_bytes,
                                       &wr_size, NULL))
#else
                        if (write(ex->in.wr_fd, ex->in_buf, buf_bytes) !=
                            buf_bytes)
#endif
                        {
                            ex->in.perrno = ERRNO;
                            buf_bytes = 0;
                            break;
                        }
                        buf_bytes = 0;
                        rec += copy_size;
                    }
                    len -= copy_size;
                }
            }
            /* if we are doing a "group by" and we have not finished all the
             * records in the first input buffer, then reset the eof state to
             * continue reading records from the first input buffer.
             */
            if ((sp->flags & SP_GROUP_BY) && t->first_in_buf)
            {
                t->input_eof = FALSE;
                t->first_group_rec = TRUE;
            }
            else
                break;  /* done with first input buffer and maybe more */
        }
        /* flush any remainder */
        if (buf_bytes != 0)
        {
#if defined(win_nt)
            if (!WriteFile(ex->in.wr_h, ex->in_buf, buf_bytes, &wr_size, NULL))
#else
            if (write(ex->in.wr_fd, ex->in_buf, buf_bytes) != buf_bytes)
#endif
                ex->in.perrno = ERRNO;
        }
    }
    
#if defined(win_nt)
    CloseHandle(ex->in.wr_h);
#else
    pthread_mutex_lock(&Global_lock);
    close(ex->in.wr_fd);
    ex->in.wr_fd = INVALID_FD;
    Global_external_count--;
    pthread_mutex_unlock(&Global_lock);
#endif
    
    if (!just_input_files)
    {
#if defined(win_nt)
        DWORD   exitCode;
        DWORD   rv;
        
        /* wait for external process */
        rv = WaitForSingleObject(pi.hProcess, INFINITE);
        if (rv != WAIT_OBJECT_0)
            return (pfunc_error(t, "process WaitForSingleObject() ret: %d, error: %s\n",
                                rv, get_error_msg(0, errmsg, sizeof(errmsg))));
        if (!GetExitCodeProcess(pi.hProcess, &exitCode))
            return (pfunc_error(t, "GetExitCodeProcess() error: %s\n",
                                get_error_msg(0, errmsg, sizeof(errmsg))));
        TRACE("pfunc_exec%d: ext proc exited with: %d\n",
              t->thread_index, exitCode);
        if (exitCode != 0)
        {
            pfunc_error(t,
                        "process '%s' instance %I64u exited with status: 0x%x\n",
                        sp->exec_argv[0], t->task_number, exitCode);
        }
        CloseHandle(pi.hProcess);
#else
        /* if error writing to pipe and is not broken pipe error */
        if (ex->in.perrno != 0 && ex->in.perrno != EPIPE)
        {
            strerror_r(ex->in.perrno, errmsg, sizeof(errmsg));
            TRACE("pfunc_exec%d: stdin write error: %d, %s\n", t->thread_index,
                  ex->in.perrno, errmsg);
            return (pfunc_error(t, "pipe stdin write error: %s\n", errmsg));
        }
    
        /* wait for child process to end */
        if (waitpid(child, &status, 0) == -1)
        {
            strerror_r(errno, errmsg, sizeof(errmsg));
            TRACE("pfunc_exec%d: child wait error: %d, %s\n",
                  t->thread_index, errno, errmsg);
            return (pfunc_error(t, "pipe process wait error: %s\n", errmsg));
        }
        if (WIFSIGNALED(status))
        {
            TRACE("pfunc_exec%d: child terminated with signal %d\n",
                  t->thread_index, WTERMSIG(status));
            return (pfunc_error(t,
                                "external process terminated with signal %d\n",
                                WTERMSIG(status)));

        }
        if (WIFSTOPPED(status))
        {
            TRACE("pfunc_exec%d: child stopped with signal %d\n",
                  t->thread_index, WSTOPSIG(status));
            return (pfunc_error(t, "external process stopped with signal %d\n",
                                WSTOPSIG(status)));
        }
        if (WIFEXITED(status) && WEXITSTATUS(status) != 0)
        {
            TRACE("pfunc_exec%d: child exited with status: %d\n",
                  t->thread_index, WEXITSTATUS(status));
            pfunc_error(t,
                        "process '%s' instance %llu exited with status: %d\n",
                        sp->exec_argv[0], t->task_number, WEXITSTATUS(status));
        }
#endif

        /* wait for stdout thread to end */
        pthread_join(out_thread, NULL);
        TRACE("pfunc_exec%d: out thread exited with errno: %d\n",
              t->thread_index, ex->out.perrno);
        if (ex->out.perrno != 0)
        {
            get_error_msg(ex->out.perrno, errmsg, sizeof(errmsg));
            TRACE("pfunc_exec%d: out thread exited on error: %d, %s\n",
                  t->thread_index, ex->out.perrno, errmsg);
            pfunc_error(t, "pipe stdout read error: %s\n", errmsg);
        }
#if defined(SUMP_PIPE_STDERR)
        /* wait for stderr thread to end */
        pthread_join(err_thread, NULL);
        TRACE("pfunc_exec%d: error thread exited with errno: %d\n",
              t->thread_index, ex->err.perrno);
        if (ex->err.perrno != 0)
        {
            get_error_msg(ex->err.perrno, errmsg, sizeof(errmsg));
            TRACE("pfunc_exec%d: error thread exited on error: %d, %s\n",
                  t->thread_index, ex->err.perrno, errmsg);
            pfunc_error(t, "pipe stderr read error: %s\n", errmsg);
        }
#endif
    }

    return (t->error_code);
}


/* file_reader_test - test routine for a file reader thread using 
 *                            normal read() calls into an intermediate
 *                            buffer followed by writes into the sump pump.
 */
static void *file_reader_test(void *arg)
{
    int                 size;
    char                *read_buf;
    sp_file_t           sp_src = (sp_file_t)arg;
    sp_t                sp = sp_src->sp;
    char                err_buf[200];
    
    TRACE("file_reader_intr: allocating %d buffer bytes\n",
          sp_src->transfer_size);
    read_buf = (char *)malloc(sp_src->transfer_size);
    if (read_buf == NULL)
    {
        sp_raise_error(sp, SP_MEM_ALLOC_ERROR,
                       "%s: buffer malloc failure, size %d\n",
                       sp_src->fname, (int)sp_src->transfer_size);
    }
    
    /* keep looping until there is no additional input */
    for ( ; read_buf != NULL; )
    {
#if defined(win_nt)
        DWORD   rlen;
        
        if (ReadFile(sp_src->fd, read_buf, sp_src->transfer_size, &rlen, NULL))
            size = rlen;        /* success */
        else if (GetLastError() == ERROR_BROKEN_PIPE)
            size = 0;           /* eof */
        else
            size = -1;          /* failure */
#else
        size = read(sp_src->fd, read_buf, (unsigned int)sp_src->transfer_size);
#endif
        if (size < 0)
        {
            sp_raise_error(sp, SP_FILE_READ_ERROR,
                           "%s: read() failure: %s\n",
                           sp_src->fname,
                           get_error_msg(0, err_buf, sizeof(err_buf)));
            break;
        }
        if (sp_write_input(sp, read_buf, size) != size)
            break;      /* silently quit on a downstream error */
        if (size == 0)  /* if we just sent 0 bytes to sp_write_input() */
            break;
    }
    if (read_buf != NULL)
        free(read_buf);
    TRACE("file_reader_test done: %d\n", sp_src->error_code);
    return (NULL);
}


/* file_reader_buffered - main routine for a file reader thread using normal
 *                        read() calls directly into sump pump input buffers.
 */
static void *file_reader_buffered(void *arg)
{
    size_t              size;
    size_t              buf_size;
    size_t              filled_bytes;
    size_t              request;
    uint64_t            index;
    char                *read_buf;
    int                 eof;
    int                 ret;
    sp_file_t           sp_src = (sp_file_t)arg;
    sp_t                sp = sp_src->sp;
    char                err_buf[200];
#if defined(win_nt)
    DWORD               rlen;
#endif

    TRACE("file_reader_buffered starting\n");
            
    /* keep looping until there is no additional input */
    for (index = 0; ; index++)
    {
        if (sp_get_in_buf(sp, index, (void **)&read_buf, &buf_size) != SP_OK)
            break;
        
        for (filled_bytes = 0; filled_bytes < buf_size; filled_bytes += size)
        {
            /* calculate read request size */
            request = buf_size - filled_bytes;
            /* limit request size to 2GB */
            if (request > 0x80000000)
                request = 0x80000000;
            if (sp_src->transfer_size != 0 && request > sp_src->transfer_size)
                request = sp_src->transfer_size;
#if defined(win_nt)
            if (ReadFile(sp_src->fd, read_buf + filled_bytes,
                         (DWORD)request, &rlen, NULL))
                size = rlen;    /* success */
            else if (GetLastError() == ERROR_BROKEN_PIPE)
                size = 0;       /* eof */
            else
                size = -1;      /* failure */
#else
            size = read(sp_src->fd, read_buf + filled_bytes,
                        (unsigned int)request);
#endif
            TRACE("file_reader: read %d bytes\n", (int)size);  
            if (size < 0)
            {
                sp_raise_error(sp, SP_FILE_READ_ERROR,
                               "%s: read() failure: %s\n",
                               sp_src->fname,
                               get_error_msg(0, err_buf, sizeof(err_buf)));
                filled_bytes = 0;
                break;
            }
            if (size == 0)  /* if EOF */
                break;
        }
        eof = (filled_bytes < request);
        if ((ret = sp_put_in_buf_bytes(sp, index, filled_bytes, eof)) != SP_OK)
        {
            TRACE("file_reader: sp_put_in_buf_bytes ret: %d\n", ret);
            break;      /* silently quit on a downstream error */
        }
        if (eof)
            break;
    }
#if defined(win_nt)
    CloseHandle(sp_src->fd);
    sp_src->fd = INVALID_HANDLE_VALUE;
#else
    /*close(sp_src->fd);*/
    sp_src->fd = INVALID_FD;
#endif
    TRACE("file_reader_buffered done: %d\n", sp_src->error_code);
    return (NULL);
}


/* file_writer_buffered - main routine for a file writer thread using normal
 *                        write() calls.
 */
static void *file_writer_buffered(void *arg)
{
    char                *buf;
    ssize_t             size;
    sp_file_t           sp_dst = (sp_file_t)arg;
    sp_t                sp = sp_dst->sp;
    int64_t             file_size = 0;
#if defined(win_nt)
    DWORD               wr_size;
#else
    int                 fd = sp_dst->fd;
#endif      
    int                 out_index = sp_dst->out_index;
    char                err_buf[200];
    
    TRACE("file_writer_buffered: allocating %d buffer bytes\n", sp_dst->transfer_size);
    buf = (char *)malloc(sp_dst->transfer_size);
    if (buf == NULL)
    {
        sp_raise_error(sp, SP_MEM_ALLOC_ERROR,
                       "%s: buffer malloc failure, size %d\n",
                       sp_dst->fname, (int)sp_dst->transfer_size);
    }
        
    for ( ; buf != NULL; )
    {
        size = sp_read_output(sp, out_index, buf, sp_dst->transfer_size);
        if (size <= 0)
        {
            if (size < 0)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
            }
            break;
        }
        TRACE("file_writer: writing %d bytes at offset %"PTFlld"\n",
              size, file_size);  
#if defined(win_nt)
        if (!WriteFile(sp_dst->fd, buf, size, &wr_size, NULL))
#else
        if (write(fd, buf, (unsigned int)size) != size)
#endif
        {
            sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                           "%s: write() failure: %s\n",
                           sp_dst->fname,
                           get_error_msg(0, err_buf, sizeof(err_buf)));
            sp_dst->error_code = SP_FILE_WRITE_ERROR;
            TRACE("output file write error: %s\n", err_buf);
            break;
        }
        file_size += size;
    }
    if (buf != NULL && sp_dst->can_seek)
    {
#if defined(win_nt)
        if (!SetEndOfFile(sp_dst->fd))
#else
        if (ftruncate(sp_dst->fd, file_size))
#endif
        {
            sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                           "%s: truncate failure: %s\n",
                           sp_dst->fname,
                           get_error_msg(0, err_buf, sizeof(err_buf)));
            sp_dst->error_code = SP_FILE_WRITE_ERROR;
            TRACE("truncate error: %s\n", err_buf);
        }
    }
    
#if defined(win_nt)
    CloseHandle(sp_dst->fd);
    sp_dst->fd = INVALID_HANDLE_VALUE;
#else
    /*close(sp_dst->fd);*/
    sp_dst->fd = INVALID_FD;
#endif
    if (buf != NULL)
        free(buf);
    TRACE("file_writer_buffered done: %d\n", sp_dst->error_code);
    return (NULL);
}


#if defined(AIO_CAPABLE)

/* file_reader_direct - main routine for a file reader thread using direct
 *                      aio_read() calls on sump pump input buffers.
 */
static void *file_reader_direct(void *arg)
{
    ssize_t             size;
    ssize_t             request;
    size_t              in_buf_size;
    uint64_t            aios_started;
    int64_t             file_read_offset = 0;
    struct sump_aio     *spaio;
    struct aiocb        *aio;
    const struct aiocb  *cb[1];
    int                 eof;
    sp_file_t           sp_src = (sp_file_t)arg;
    sp_t                sp = sp_src->sp;
    char                err_buf[200];
    int                 aio_count;
    int                 start;
    int                 done;
    uint64_t            next_in_buf;
    size_t              next_buf_offset;
    char                *buf;
    int                 put_result;
#if defined(win_nt)
    int                 i;
#endif

    if (sp_src->aio_count <= 0)
        aio_count = 2;
    else
        aio_count = sp_src->aio_count;
    TRACE("file_reader_direct allocating %d aio structs\n", aio_count);
    spaio = (struct sump_aio *)calloc(sizeof(struct sump_aio), aio_count);
    if (spaio == NULL)
    {
        sp_raise_error(sp, SP_MEM_ALLOC_ERROR,
                       "%s: aio malloc failure, size %d\n",
                       sp_src->fname, (int)aio_count);
        TRACE("file_reader_direct done: %d\n", sp_src->error_code);
        return (NULL);
    }
#if defined(win_nt)
    for (i = 0; i < aio_count; i++)
    {
        spaio[i].aio.sump_over.hEvent = CreateEvent(NULL, 1, 0, NULL);
        if (spaio[i].aio.sump_over.hEvent == NULL)
            die("file_reader_direct, CreateEvent failed\n");
    }
#endif
    
    /* keep looping until there is no additional input */
    next_in_buf = 0;
    next_buf_offset = 0;
    for (aios_started = 0; ; aios_started++)
    {
        start = aios_started % aio_count;
        aio = &spaio[start].aio;
        aio->aio_fildes = sp_src->fd;
        /* if getting of the buffer fails. */
        if (sp_get_in_buf(sp, next_in_buf, (void **)&buf, &in_buf_size) != SP_OK)
        {
            sp_raise_error(sp, SP_FILE_READ_ERROR,
                           "sp_get_in_buf() failure with in_buf %lld\n",
                           aios_started);
            break;
        }
        request = in_buf_size - next_buf_offset;
        if (request > sp_src->transfer_size)
            request = sp_src->transfer_size;
        aio->aio_buf = buf + next_buf_offset;
        aio->aio_nbytes = request;
        aio->aio_offset = file_read_offset;
        spaio[start].buf_index = next_in_buf;
        spaio[start].buf_offset = next_buf_offset;
        spaio[start].file_offset = file_read_offset;
        spaio[start].nbytes = request;
        spaio[start].last_buf_io = (next_buf_offset + request == in_buf_size);
        next_buf_offset += request;
        TRACE("file_reader: reading %d bytes at offset %"PTFlld"\n",
              request, file_read_offset);
        if (next_buf_offset == in_buf_size)
        {
            next_in_buf++;
            next_buf_offset = 0;
        }
        if (aio_read(aio) < 0)
        {
            get_error_msg(aio_error(aio), err_buf, sizeof(err_buf));
            TRACE("file_reader_direct: read failed: %s\n", err_buf);
            sp_src->error_code = SP_FILE_READ_ERROR;
            sp_raise_error(sp, SP_FILE_READ_ERROR,
                           "%s: aio_read() failure: %s, "
                           "offset: %"PTFlld", size: %"PTFlld"\n",
                           sp_src->fname, err_buf,
                           aio->aio_offset, (int64_t)request);
            break;
        }
        file_read_offset += request;
        /* if the max number of aios has not been started, then start another.
         */
        if (aios_started < aio_count - 1)
            continue;

        /* wait for a previously issued request.
         */
        done = (aios_started + 1) % aio_count;
        aio = &spaio[done].aio;
        request = spaio[done].nbytes;
        cb[0] = aio;
        if (aio_suspend(cb, 1, NULL) != 0)
        {
            sp_src->error_code = SP_FILE_READ_ERROR;
            sp_raise_error(sp, SP_FILE_READ_ERROR,
                           "%s: aio_suspend() failure: %s, "
                           "offset: %"PTFlld", size: %"PTFlld"\n",
                           sp_src->fname,
                           get_error_msg(0, err_buf, sizeof(err_buf)),
                           spaio[done].file_offset, (int64_t)request);
            break;
        }
        if ((size = aio_return(aio)) < 0)
        {
            sp_src->error_code = SP_FILE_READ_ERROR;
            sp_raise_error(sp, SP_FILE_READ_ERROR,
                           "%s: aio_return() failure: %s, "
                           "offset: %"PTFlld", size: %"PTFlld"\n",
                           sp_src->fname,
                           get_error_msg(aio_error(aio), err_buf,
                                         sizeof(err_buf)),
                           spaio[done].file_offset, (int64_t)request);
            break;
        }

        eof = (size < request);
        
        /* "put" the input buffer bytes if some bytes have been read into
         * the buffer and either eof or that was the last read for the buffer
         */
        size += spaio[done].buf_offset;
        put_result = SP_OK;
        if (size && (eof || spaio[done].last_buf_io))
        {
            put_result =
                sp_put_in_buf_bytes(sp, spaio[done].buf_index, size, eof);
        }
        if (put_result != SP_OK || eof)
        {
            /* clean up remaining aios and finish up */
            if (aio_count != 1)
            {
                /* wait for and ignore all previously issued aio'ess
                 */
                do
                {
                    done = (done + 1) % aio_count;
                    cb[0] = &spaio[done].aio;
                    aio_suspend(cb, 1, NULL);
                } while (done != start);
            }
            break;
        }
    }
#if defined(win_nt)
    for (i = 0; i < aio_count; i++)
        CloseHandle(spaio[i].aio.sump_over.hEvent);
    CloseHandle(sp_src->fd);
    sp_src->fd = INVALID_HANDLE_VALUE;
#else
    /*close(sp_src->fd);*/
    sp_src->fd = INVALID_FD;
#endif
    free(spaio);
    TRACE("file_reader_direct done: %d\n", sp_src->error_code);
    return (NULL);
}


/* file_writer_direct - main routine for a file writer thread using direct
 *                      aio_write() calls.
 */
static void *file_writer_direct(void *arg)
{
    char                *buf;
    ssize_t             size;
    ssize_t             alloc_size;
    sp_file_t           sp_dst = (sp_file_t)arg;
    sp_t                sp = sp_dst->sp;
    int                 out_index = sp_dst->out_index;
    ssize_t             request;
    uint64_t            aios_started;
    uint64_t            aios_completed;
    int64_t             file_write_offset = 0;
    struct sump_aio     *spaio;
    struct aiocb        *aio;
    const struct aiocb  *cb[1];
    int                 aio_count;
    char                err_buf[200];
    int                 start;
    int                 done;
    int                 eof;
    int                 ret;
    char                *remainder_start = NULL;
    int64_t             remainder_size = 0;
#if defined(win_nt)
    LONG                high;
    LONG                low;
    int                 i;
#endif

    if (sp_dst->aio_count <= 0)
        aio_count = 2;
    else
        aio_count = sp_dst->aio_count;
    spaio = (struct sump_aio *)calloc(sizeof(struct sump_aio), aio_count);
    if (spaio == NULL)
    {
        sp_raise_error(sp, SP_MEM_ALLOC_ERROR,
                       "%s: aio malloc failure, size %d\n",
                       sp_dst->fname, (int)aio_count);
        TRACE("file_writer_direct done: %d\n", sp_dst->error_code);
        return (NULL);
    }

    TRACE("file_writer_direct[%d]: allocating %d buffer bytes\n",
          out_index, sp_dst->transfer_size);
    alloc_size = sp_dst->transfer_size * aio_count;
#if defined(win_nt)
    buf = VirtualAlloc(NULL, alloc_size, MEM_COMMIT, PAGE_READWRITE);
    if (buf == NULL)
    {
        sp_raise_error(sp, SP_MEM_ALLOC_ERROR,
                       "VirtualAlloc() failure: %s\n",
                       get_error_msg(0, err_buf, sizeof(err_buf)));
        sp_dst->error_code = SP_MEM_ALLOC_ERROR;
        return NULL;
    }
    for (i = 0; i < aio_count; i++)
    {
        spaio[i].aio.sump_over.hEvent = CreateEvent(NULL, 1, 0, NULL);
        if (spaio[i].aio.sump_over.hEvent == NULL)
            die("file_writer_direct, CreateEvent failed\n");
    }
#else
    init_zero_fd();
    buf = mmap(NULL, alloc_size, PROT_READ | PROT_WRITE, MAP_PRIVATE, Zero_fd, 0);
    if (buf == MAP_FAILED)
    {
        sp_raise_error(sp, SP_MEM_ALLOC_ERROR,
                       "mmap() failure: %s\n",
                       strerror_r(errno, err_buf, sizeof(err_buf)));
        sp_dst->error_code = SP_MEM_ALLOC_ERROR;
        return NULL;
    }
#endif

    aios_completed = 0;
    for (aios_started = 0; ; )
    {
        start = aios_started % aio_count;
        aio = &spaio[start].aio;
        aio->aio_fildes = sp_dst->fd;
        aio->aio_buf = buf + start * sp_dst->transfer_size;
        request = sp_read_output(sp, out_index, (void *)aio->aio_buf,
                              sp_dst->transfer_size);
        if (request < 0)
        {
            sp_dst->error_code = SP_FILE_WRITE_ERROR;
            break;
        }

        /* if not a write or not a full write, then its eof.
         */
        eof = (request < sp_dst->transfer_size);
        if (eof &&
            (remainder_size = request % PAGE_SIZE) != 0)
        {
            request -= remainder_size;
            remainder_start = (char *)aio->aio_buf + request;
        }
        if (request != 0)
        {
            TRACE("file_writer[%d]: writing %"PTFlld" bytes at offset %"PTFlld"\n",
                  out_index, (int64_t)request, file_write_offset);  
            aio->aio_nbytes = request;
            aio->aio_offset = file_write_offset;
            spaio[start].buf_index = 0;    /* not used */
            spaio[start].buf_offset = 0;   /* not used */
            spaio[start].file_offset = file_write_offset;
            spaio[start].nbytes = request;
            spaio[start].last_buf_io = 0;  /* not used */
            if (aio_write(aio) < 0)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
                sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                               "%s: aio_write() failure: %s, "
                               "offset: %lld, size: %lld\n",
                               sp_dst->fname,
                               get_error_msg(aio_error(&aio[start]), err_buf,
                                             sizeof(err_buf)),
                               spaio[done].file_offset, request);
                break;
            }
            file_write_offset += request;
            aios_started++;
        }
        else if (aios_started == 0)  /* if no direct output whatsoever */
            break;
        
        /* if not eof and the max number of aios has not been started,
         * then start another.
         */
        if (!eof && aios_started < aio_count)
            continue;

        /* wait for a previously issued request.
         * if eof, then wait for all previously issued requests.
         */
        do
        {
            done = aios_completed % aio_count;
            aio = &spaio[done].aio;
            request = spaio[done].nbytes;
            cb[0] = aio;
#if defined(win_nt)
            ret = aio_suspend(cb, 1, NULL);
#else
            while ((ret = aio_suspend(cb, 1, NULL)) != 0 && errno == EINTR)
                continue;
#endif
            if (ret != 0)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
                sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                               "%s: aio_suspend() failure: %s, "
                               "offset: %lld, size: %lld\n",
                               sp_dst->fname,
                               get_error_msg(0, err_buf, sizeof(err_buf)),
                               spaio[done].file_offset, request);
                break;
            }
            if ((size = aio_return(aio)) < 0)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
                sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                               "%s: aio_return() failure: %s, "
                               "offset: %lld, size: %lld\n",
                               sp_dst->fname,
                               get_error_msg(aio_error(aio), err_buf,
                                             sizeof(err_buf)),
                               spaio[done].file_offset, request);
                break;
            }
            if (size != request)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
                sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                               "%s: aio_write() return failure: %s, "
                               "offset: %"PTFlld", "
                               "returned size %"PTFlld
                               " != requested size: %"PTFlld"\n",
                               sp_dst->fname,
                               get_error_msg(aio_error(aio), err_buf,
                                             sizeof(err_buf)),
                               spaio[done].file_offset,
                               (int64_t)size, (int64_t)request);
                break;
            }
            aios_completed++;
        } while (eof && aios_completed < aios_started);
        if (eof || sp_dst->error_code != 0)
            break;
    }
    if (sp_dst->error_code == 0 && remainder_size != 0)
    {
        TRACE("file_writer[%d]: writing remaining %"PTFlld" bytes at offset %"PTFlld"\n",
              out_index, (int64_t)remainder_size, file_write_offset);
#if defined(win_nt)
        CloseHandle(sp_dst->fd);
        sp_dst->fd = CreateFile(sp_dst->fname,
                                GENERIC_WRITE,
                                FILE_SHARE_READ,
                                NULL,
                                OPEN_ALWAYS,
                                FILE_ATTRIBUTE_NORMAL,
                                NULL);
        if (sp_dst->fd == INVALID_HANDLE_VALUE)
        {
            sp_dst->error_code = SP_FILE_WRITE_ERROR;
            sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                           "%s: remainder CreateFile() return failure: %s\n",
                           sp_dst->fname,
                           get_error_msg(0, err_buf, sizeof(err_buf)));
        }
        else
        {
            int err, wr_ret;
            
            aio = &spaio[0].aio;
            aio->sump_over.Offset = (int)file_write_offset;
            aio->sump_over.OffsetHigh = (int)(file_write_offset >> 32);
            ResetEvent(aio->sump_over.hEvent);
            wr_ret = WriteFile(sp_dst->fd, remainder_start, remainder_size,
                               NULL, &aio->sump_over);
            err = GetLastError();
            if (wr_ret == 0 && err != ERROR_IO_PENDING)
                ret = -1;
            else
            {
                wr_ret = GetOverlappedResult(sp_dst->fd, &aio->sump_over,
                                             &ret, TRUE);
                if (wr_ret == 0)
                    ret = -1;
            }

            if (ret != remainder_size)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
                sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                               "%s: pwrite() return failure: %s, "
                               "offset: %I64d, "
                               "returned size %d != requested size: %I64d\n",
                               sp_dst->fname,
                               get_error_msg(0, err_buf, sizeof(err_buf)),
                               file_write_offset, ret, remainder_size);
            }
            else
                file_write_offset += remainder_size;
        }
#else
        /* close file descriptor that was opened with O_DIRECT */
        close(sp_dst->fd);
        /* reopen file without O_DIRECT */
        if ((sp_dst->fd = open(sp_dst->fname, O_WRONLY, 0777)) < 0)
        {
            sp_dst->error_code = SP_FILE_WRITE_ERROR;
            sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                           "%s: remainder open() return failure: %s\n",
                           sp_dst->fname,
                           strerror_r(errno, err_buf, sizeof(err_buf)));
        }
        else
        {
            ret = pwrite(sp_dst->fd, remainder_start,
                         remainder_size, file_write_offset);
            if (ret != remainder_size)
            {
                sp_dst->error_code = SP_FILE_WRITE_ERROR;
                sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                               "%s: pwrite() return failure: %s, "
                               "offset: %lld, "
                               "returned size %d != requested size: %lld\n",
                               sp_dst->fname,
                               strerror_r(errno, err_buf, sizeof(err_buf)),
                               file_write_offset, ret, remainder_size);
            }
            else
                file_write_offset += remainder_size;
        }
#endif
    }
#if defined(win_nt)
    /* truncate output file, since we didn't truncate it when we opened it. */
    high = (LONG)(file_write_offset >> 32);
    low = (LONG)(file_write_offset & 0xFFFFFFFF);
    ret = SetFilePointer(sp_dst->fd, low, &high, FILE_BEGIN);
    if (ret == INVALID_SET_FILE_POINTER && GetLastError() != NO_ERROR)
    {
        sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                       "%s: SetFilePointer() failure: %s\n",
                       sp_dst->fname,
                       get_error_msg(0, err_buf, sizeof(err_buf)));
        sp_dst->error_code = SP_FILE_WRITE_ERROR;
        TRACE("file_writer_direct[%d]: SetFilePointer() error: %s\n",
              out_index, err_buf);
    }
    else if (!SetEndOfFile(sp_dst->fd))
    {
        sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                       "%s: SetEndOfFile() failure: %s\n",
                       sp_dst->fname,
                       get_error_msg(0, err_buf, sizeof(err_buf)));
        sp_dst->error_code = SP_FILE_WRITE_ERROR;
        TRACE("file_writer_direct[%d]: SetEndOfFile() error: %s\n",
              out_index, err_buf);
    }
    else
        TRACE("file_writer_direct[%d]: truncated file to %I64d bytes\n",
              out_index, file_write_offset);

    VirtualFree(buf, alloc_size, MEM_RELEASE);
    for (i = 0; i < aio_count; i++)
        CloseHandle(spaio[i].aio.sump_over.hEvent);
    CloseHandle(sp_dst->fd);
    sp_dst->fd = INVALID_HANDLE_VALUE;
#else
    if (ftruncate(sp_dst->fd, file_write_offset))
    {
        sp_raise_error(sp, SP_FILE_WRITE_ERROR,
                       "%s: ftruncate() failure: %s\n",
                       sp_dst->fname,
                       get_error_msg(0, err_buf, sizeof(err_buf)));
        sp_dst->error_code = SP_FILE_WRITE_ERROR;
        TRACE("file_writer_direct[%d]: ftruncate() error: %s\n",
              out_index, err_buf);
    }
    
    munmap(buf, alloc_size);
    /*close(sp_dst->fd);*/
    sp_dst->fd = INVALID_FD;
#endif
    free(spaio);
    TRACE("file_writer_direct[%d] done: %d\n", out_index, sp_dst->error_code);
    return NULL;
}

#endif


/* sp_wait - can be called by an external thread, e.g. the thread that
 *           called sp_start(), to wait for all sump pump activity to cease.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_wait(sp_t sp)
{
    unsigned    i;
    int         ret;

    /* if already waited for this sump pump to complete */
    if (sp->wait_done == TRUE)
        return (sp->error_code);
    if (sp->flags & SP_SORT)
    {
#if !defined(SUMP_PUMP_NO_SORT)
        /* if the sort output is going to a file, and we haven't yet
         * waited for it to complete.
         */
        if (sp->sort_state == SORT_OUTPUT && sp->out[0].file != NULL)
        {
            size_t  zero = 0;
            
            ret = (*Nsort_return_recs)(NULL, &zero, &sp->nsort_ctx);
            if (ret != NSORT_END_OF_OUTPUT)
                post_nsort_error(sp, ret);
            sp->sort_state = SORT_DONE;
        }
#endif
        pthread_mutex_lock(&sp->sump_mtx);
        while (sp->error_code == 0 && sp->sort_state != SORT_DONE)
        {
            TRACE("sp_wait() condition wait");
            pthread_cond_wait(&sp->task_output_ready_cond, &sp->sump_mtx);
        }
        pthread_mutex_unlock(&sp->sump_mtx);
        return (sp->error_code);
    }
    else /* normal (non-sort) sump pump */
    {
        TRACE("waiting for input file reader\n");
        if (sp->in_file_sp != NULL)
        {
            if ((ret = sp_file_wait(sp->in_file_sp)) != SP_OK)
                return (sp->error_code == SP_OK ? ret : sp->error_code);
        }
        for (i = 0; i < sp->num_threads; i++)
        {
            TRACE("waiting for pump thread %d\n", i);
            pthread_join(sp->thread[i], NULL);
        }
        for (i = 0; i < sp->num_outputs; i++)
        {
            if (sp->out[i].file_sp != NULL)
            {
                TRACE("waiting for output %d thread\n", i);
                if ((ret = sp_file_wait(sp->out[i].file_sp)) != SP_OK)
                    return (sp->error_code == SP_OK ? ret : sp->error_code);
            }
        }
    }
    sp->wait_done = TRUE;
    return (sp->error_code);
}


/* syntax error - internal routine to internally record a syntax error
 *                message for possible later retrieval.
 */
static void syntax_error(sp_t sp, char *p, char *err_msg)
{
    int         ret;
    const char  *fmt = "syntax error: %s at: \"%s\"\n";

    if (sp->error_code != 0)          /* if prior error */
        return;                       /* ignore this one */
    sp->error_code = SP_SYNTAX_ERROR;

#if defined(win_nt)
    ret = _snprintf(sp->error_buf, sp->error_buf_size, fmt, err_msg, p);
    if (ret == -1)
        ret = _scprintf(fmt, err_msg, p);
#else
    ret = snprintf(sp->error_buf, sp->error_buf_size, fmt, err_msg, p);
#endif
    if ((size_t)ret >= sp->error_buf_size)
    {
        if (sp->error_buf_size != 0)
            free(sp->error_buf);
        sp->error_buf_size = (size_t)ret + 1;
        sp->error_buf = (char *)malloc(sp->error_buf_size);
#if defined(win_nt)
        _snprintf(sp->error_buf, sp->error_buf_size, fmt, err_msg, p);
#else
        snprintf(sp->error_buf, sp->error_buf_size, fmt, err_msg, p);
#endif
    }
    
    return;
}


/* get_numeric_arg - internal routine to convert an ascii number to int64_t
 */
static int64_t get_numeric_arg(sp_t sp, char **caller_p)
{
    int         negative = 0;
    int64_t     result = 0;
    char        *p = *caller_p;

    if (*p == '-')
    {
        p++;
        negative = 1;
    }
    if (*p < '0' || *p > '9')
    {
        syntax_error(sp, p, "expected numeric argument");
        return 0;
    }
    while (*p >= '0' && *p <= '9')
    {
        result = result * 10 + (*p - '0');
        p++;
    }
    *caller_p = p;
    return (negative ? -result : result);
}


/* get_scale - internal routine to convert 'k', 'm' of 'g' to a numeric value
 */
static int64_t get_scale(char **caller_p)
{
    char        *p = *caller_p;
    int64_t     factor = 1;

    if (*p == 'k' || *p == 'K')
        factor = 1024;
    else if (*p == 'm' || *p == 'M')
        factor = 1024 * 1024;
    else if (*p == 'g' || *p == 'G')
        factor = 1024 * 1024 * 1024;
    else
        return (1);
    *caller_p = p + 1;
    return (factor);
}


/* scan - internal routine to scan the sp_start() string for a keyword.
 */
static int scan(char *kw, char **dp)
{
    unsigned char       *kp;
    unsigned char       *p;

    kp = (unsigned char *)kw;
    p = *(unsigned char **)dp;
    
    /* while we haven't hit the end of the keyword or the end of the
     * definition string, and
     * there is a either a match in the current characters or the keyword
     * string contains an '_' and if we skip over it the chars match.
     */
    while (*kp != '\0' &&
           *p != '\0' &&
           (toupper(*kp) == toupper(*p) ||
            (*kp == '_' && (kp++, toupper(*kp) == toupper(*p)))))
    {
        /* go on to next characters in each string */
        kp++;
        p++;
    }

    /* there was a match if we got to the end of the keyword string and
     * either the last character of the keyword was '=' or the ending
     * definition string character is not alphabetic nor underscore
     */
    if (*kp == '\0' &&
        (*(kp - 1) == '=' ||
         !((toupper(*p) >= 'A' && toupper(*p) <= 'Z') || *p == '_')))
    {
        /* update definition pointer */
        *dp = (char *)p;
        return TRUE;
    }
    else
        return FALSE;
}


/* get_file_mods - internal routine to get file name modifiers,
 *                 e.g. access mode and transfer size.
 */
static void get_file_mods(sp_file_t spf, char *mods)
{
    char        *p = mods;

    for (;;)
    {
        while (*p == ',')
            p++;
        if (*p == '\0')
            break;
        else if (scan("BUFFERED", &p) || scan("BUF", &p))
        {
            spf->mode = MODE_BUFFERED;
        }
        else if (scan("DIRECT", &p) || scan("DIR", &p))
        {
            spf->mode = MODE_DIRECT;
        }
        else if (scan("COUNT", &p) || scan("CO", &p))
        {
            if (*p != ':' && *p != '=')
            {
                syntax_error(spf->sp, p, "expected ':' or '=' after 'count'");
                return;
            }
            p++;
            spf->aio_count = (int)get_numeric_arg(spf->sp, &p);
        }
        else if (scan("TRANSFER", &p) || scan("TRANS", &p) || scan("TR", &p))
        {
            if (*p != ':' && *p != '=')
            {
                syntax_error(spf->sp, p, "expected ':' of '='");
                return;
            }
            p++;
            spf->transfer_size = (size_t)get_numeric_arg(spf->sp, &p);
            spf->transfer_size *= (size_t)get_scale(&p);
        }
        else
        {
            syntax_error(spf->sp, p, "unrecognized file modifier");
            break;
        }
    }
}


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
sp_file_t sp_open_file_src(sp_t sp, const char *fname_mods)
{
    sp_file_t   sp_src;
    char        *comma_char;
    int         fname_len;
    void        *(*reader_main)(void *);
    int         is_stdin;
    int         specified_mode;

    if ((sp_src = (sp_file_t)calloc(1, sizeof(struct sp_file))) == NULL)
        return (NULL);
    sp_src->sp = sp;

    comma_char = strchr(fname_mods, ',');
    fname_len = (int)
        (comma_char == NULL ? strlen(fname_mods) : comma_char - fname_mods);
    sp_src->fname = (char *)calloc(1, fname_len + 1);
    memcpy(sp_src->fname, fname_mods, fname_len);
    sp_src->fname[fname_len] = '\0';
    if (comma_char != NULL)
        get_file_mods(sp_src, comma_char + 1);
    
    specified_mode = sp_src->mode;
    is_stdin = (strcmp(sp_src->fname, "<stdin>") == 0);
#if defined(win_nt)
    if (is_stdin)
    {
        sp_src->fd = GetStdHandle(STD_INPUT_HANDLE);
        if (sp_src->transfer_size == 0)
            /* there is a read size limit for keyboard input, but is this it?*/
            sp_src->transfer_size = 8192;
        sp_src->mode = MODE_BUFFERED;
    }
    else
    {
        sp_src->fd = CreateFile(sp_src->fname,
                                GENERIC_READ,
                                FILE_SHARE_READ,
                                NULL,
                                OPEN_EXISTING,
                                FILE_ATTRIBUTE_NORMAL,
                                NULL);
        if (sp_src->fd == INVALID_HANDLE_VALUE)
            return (NULL);
        sp_src->can_seek = (GetFileType(sp_src->fd) == FILE_TYPE_DISK);
    }
#else
    if (is_stdin)
    {
        sp_src->fd = 0;
        sp_src->mode = MODE_BUFFERED;
    }        
    else
    {
        struct stat     buf;
        
        sp_src->fd = open(sp_src->fname, 0);
        if (sp_src->fd < 0)
            return (NULL);
        if (fstat(sp_src->fd, &buf) != 0)
            return (NULL);
        sp_src->can_seek = S_ISREG(buf.st_mode);
    }
#endif

    /* if direct mode was specified, but it is not a file */
    if (specified_mode == MODE_DIRECT && !sp_src->can_seek)
    {
        start_error(sp, "direct mode reads were requested for file %s, but it"
                    " is either not a normal file or stdin\n", sp_src->fname);
        return (NULL);
    }

    if (sp_src->mode == MODE_UNSPECIFIED)
        sp_src->mode = sp_src->can_seek ? Default_file_mode : MODE_BUFFERED;

    /* if file mode is direct (whether by specification or default)
     */
    if (sp_src->mode == MODE_DIRECT)
    {
        /* if a transfer size has been specified that is not a multiple of the
         * page size, then silently revert to buffered mode.
         * We probably should instead stay with direct mode, allocate separate
         * read buffers, and copy the data into the sump pump input buffers.
         */
        if (sp_src->transfer_size != 0 &&
            sp_src->transfer_size % PAGE_SIZE != 0)
        {
            sp_src->mode = MODE_BUFFERED;
            start_error(sp, "direct mode reads for file %s,"
                        " but the specified transfer size is not a "
                        "multiple of the page size\n", sp_src->fname);
            return (NULL);
        }
        /* if transfer size has been set and the input buffer size is not a
         * multiple of it, then default to buffered.  This is because the 
         * input buffers are directly read into.
         * We could use vectored reads to eliminate this restriction.
         */
        if (sp_src->transfer_size != 0 &&
            sp->in_buf_size % sp_src->transfer_size != 0)
        {
            sp_src->mode = MODE_BUFFERED;
            start_error(sp, "direct mode reads for file %s, but "
                        "the sump pump input buffer size is not a multiple "
                        "of the specified transfer size\n", sp_src->fname);
            return (NULL);
        }
        /* if the input buffer size is not a multiple of the page size
         */
        if (sp->in_buf_size % PAGE_SIZE != 0)
        {
            /* flag error if direct was specified, otherwise use buffered */
            if (specified_mode == MODE_DIRECT)
            {
                start_error(sp,
                            "direct mode reads were specified for file %s, "
                            "but the sump pump input buffer size is not a "
                            "multiple of the page size\n", sp_src->fname);
                return (NULL);
            }
            sp_src->mode = MODE_BUFFERED;
        }
    }

#if defined(AIO_CAPABLE)
    if (sp_src->can_seek && sp_src->mode == MODE_DIRECT)
    {
        reader_main = file_reader_direct;
        if (sp_src->transfer_size == 0)
            sp_src->transfer_size = 512 * 1024; /* probably should be larger
                                                 * for Windows. */
        if (sp_src->aio_count == 0)
            sp_src->aio_count = 4;

        /* now close file and reopen it as direct */
# if defined(win_nt)
        CloseHandle(sp_src->fd);
        sp_src->fd = CreateFile(sp_src->fname,
                                GENERIC_READ,
                                FILE_SHARE_READ,
                                NULL,
                                OPEN_EXISTING,
                                FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,
                                NULL);
        if (sp_src->fd == INVALID_HANDLE_VALUE)
            return (NULL);
# else
        close(sp_src->fd);
        sp_src->fd = open(sp_src->fname, O_DIRECT);
        if (sp_src->fd < 0)
            return (NULL);
# endif
    }
    else
#endif
    {
        if (Default_rw_test_size != 0)
        {
            /* test mode for some regression tests */
            reader_main = file_reader_test;
            sp_src->transfer_size = Default_rw_test_size;
        }
        else
            reader_main = file_reader_buffered;
        if (sp_src->transfer_size == 0)
        {
            if (sp_src->can_seek)  /* if normal file */
                sp_src->transfer_size = DEFAULT_BUFFERED_TRANSFER_SIZE;
            else
                sp_src->transfer_size = DEFAULT_PIPE_TRANSFER_SIZE;
        }
    }
    
    /* create reader thread */
    if (pthread_create(&sp_src->thread, NULL, reader_main, sp_src) != 0)
        return (NULL);
    return (sp_src);
}


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
sp_file_t sp_open_file_dst(sp_t sp, unsigned out_index, const char *fname_mods)
{
    sp_file_t           sp_dst;
    int                 ret;
    char                *comma_char;
    int                 fname_len;
    void                *(*writer_main)(void *);
    int                 specified_mode;

    if ((sp_dst = (sp_file_t)calloc(1, sizeof(struct sp_file))) == NULL)
        return (NULL);
    sp_dst->sp = sp;

    comma_char = strchr(fname_mods, ',');
    fname_len = (int)
        (comma_char == NULL ? strlen(fname_mods) : comma_char - fname_mods);
    sp_dst->fname = (char *)calloc(1, fname_len + 1);
    memcpy(sp_dst->fname, fname_mods, fname_len);
    sp_dst->fname[fname_len] = '\0';
    if (comma_char != NULL)
        get_file_mods(sp_dst, comma_char + 1);

    specified_mode = sp_dst->mode;
#if defined(win_nt)
    if (strcmp(sp_dst->fname, "<stdout>") == 0)
        sp_dst->fd = GetStdHandle(STD_OUTPUT_HANDLE);
    else if (strcmp(sp_dst->fname, "<stderr>") == 0)
        sp_dst->fd = GetStdHandle(STD_ERROR_HANDLE);
    else
    {
        sp_dst->fd = CreateFile(sp_dst->fname,
                                GENERIC_WRITE,
                                FILE_SHARE_READ,
                                NULL,
                                OPEN_ALWAYS,
                                FILE_ATTRIBUTE_NORMAL,
                                NULL);
        if (sp_dst->fd == INVALID_HANDLE_VALUE)
            return (NULL);
        sp_dst->can_seek = (GetFileType(sp_dst->fd) == FILE_TYPE_DISK);
    }
#else
    if (strcmp(sp_dst->fname, "<stdout>") == 0)
        sp_dst->fd = 1;
    else if (strcmp(sp_dst->fname, "<stderr>") == 0)
        sp_dst->fd = 2;
    else
    {
        struct stat     buf;
        
        sp_dst->fd = open(sp_dst->fname, O_WRONLY | O_CREAT, 0777);
        if (sp_dst->fd < 0)
            return (NULL);
        if (fstat(sp_dst->fd, &buf) != 0)
            return (NULL);
        sp_dst->can_seek = S_ISREG(buf.st_mode);
    }
#endif

    /* if direct mode was specified but file is not a normal file */
    if (specified_mode == MODE_DIRECT && !sp_dst->can_seek)
    {
        start_error(sp, "direct mode writes were requested for file %s, but it"
                    " is either not a file or stdout/stderr\n", sp_dst->fname);
        return (NULL);
    }

    if (sp_dst->mode == MODE_UNSPECIFIED)
        sp_dst->mode = sp_dst->can_seek ? Default_file_mode : MODE_BUFFERED;

    /* if file mode is direct (whether by specification or default)
     */
    if (sp_dst->mode == MODE_DIRECT)
    {
        /* if a transfer size has been specified that is not a multiple of the
         * page size, then silently revert to buffered mode.
         * We probably should instead stay with direct mode, allocate separate
         * read buffers, and copy the data into the sump pump input buffers.
         */
        if (sp_dst->transfer_size != 0 &&
            sp_dst->transfer_size % PAGE_SIZE != 0)
        {
            sp_dst->mode = MODE_BUFFERED;
            start_error(sp, "direct mode writes for file %s"
                        ", but the transfer size is not a "
                        "multiple of the page size\n", sp_dst->fname);
            return (NULL);
        }
    }

#if defined(AIO_CAPABLE)
    if (sp_dst->can_seek && sp_dst->mode == MODE_DIRECT)
    {
        writer_main = file_writer_direct;
        if (sp_dst->transfer_size == 0)
            sp_dst->transfer_size = 512 * 1024;
        if (sp_dst->aio_count == 0)
            sp_dst->aio_count = 4;

        /* now close file and reopen it as direct */
# if defined(win_nt)
        CloseHandle(sp_dst->fd);
        sp_dst->fd = CreateFile(sp_dst->fname,
                                GENERIC_WRITE,
                                FILE_SHARE_READ,
                                NULL,
                                OPEN_ALWAYS,
                                FILE_FLAG_NO_BUFFERING | FILE_FLAG_OVERLAPPED,
                                NULL);
        if (sp_dst->fd == INVALID_HANDLE_VALUE)
            return (NULL);
# else
        close(sp_dst->fd);
        sp_dst->fd = open(sp_dst->fname, O_DIRECT | O_WRONLY | O_CREAT, 0777);
        if (sp_dst->fd < 0)
            return (NULL);
# endif
    }
    else
#endif
    {
        writer_main = file_writer_buffered;
        if (sp_dst->transfer_size == 0)
        {
            if (Default_rw_test_size != 0)
                sp_dst->transfer_size = Default_rw_test_size;
            else if (sp_dst->can_seek)  /* if normal file */
                sp_dst->transfer_size = DEFAULT_BUFFERED_TRANSFER_SIZE;
            else
                sp_dst->transfer_size = DEFAULT_PIPE_TRANSFER_SIZE;
        }
    }
    
    sp_dst->out_index = out_index;

    /* create writer thread */
    if ((ret = pthread_create(&sp_dst->thread, NULL, writer_main, sp_dst)))
        die("sp_open_file_dst: pthread_create() ret: %d\n", ret);
    return (sp_dst);
}


/* sp_file_wait - wait for the specified file connection to complete.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_file_wait(sp_file_t sp_file)
{
    if (sp_file->wait_done == TRUE)
        return (sp_file->error_code);
    pthread_join(sp_file->thread, NULL);
    sp_file->wait_done = TRUE;
    return (sp_file->error_code);
}


/* check_task_done - internal routine to make sure there is room for at
 *                   least one new task.  
 *                   Caller must have locked sump_mtx.
 */
static void check_task_done(sp_t sp)
{
    sp_task_t   t;

    TRACE("check_task_done() called\n");

    /* while there are tasks which have not yet been recognized as done.
     */
    if (sp->cnt_task_init > sp->cnt_task_done)
    {
        /* while there is no room for a new task
         */
        while (sp->error_code == 0 &&
               (sp->cnt_task_init >
                sp->cnt_task_drained + sp->num_tasks - 1))
        {
            TRACE("check_task_done() condition wait for task %d\n",
                  sp->cnt_task_drained);
            pthread_cond_wait(&sp->task_drained_cond, &sp->sump_mtx);
        }
        if (sp->error_code != 0)
        {
            TRACE("check_task_done() returning because of error_code: %d\n",
                  sp->error_code);
            return;
        }

        /* Verify the actual ending position of each done task
         * matches its expected ending position.
         */
        while (sp->cnt_task_drained > sp->cnt_task_done)
        {
            t = &sp->task[sp->cnt_task_done % sp->num_tasks];
            TRACE("check_task_done() task %d verify\n",
                  sp->cnt_task_done);
            if (t->curr_in_buf_index != t->expected_end_index ||
                (t->curr_rec - t->in_buf) != t->expected_end_offset)
            {
                die("task %d input ending mismatch: "
                    "ci %d, ei %d, ao %d, eo %d\n",
                    sp->cnt_task_done,
                    t->curr_in_buf_index,
                    t->expected_end_index,
                    (t->curr_rec - t->in_buf),
                    t->expected_end_offset);
            }
            sp->cnt_task_done++;
        }
    }
    TRACE("check_task_done() returning\n");
}


/* init_new_task - internal routine called by sp_write_input() to
 *                 initialize a new sump pump task.
 */
static sp_task_t init_new_task(sp_t sp, in_buf_t *ib, char *curr_rec)
{            
    sp_task_t           t;
    unsigned            i;

    t = &sp->task[sp->cnt_task_init % sp->num_tasks];
    t->task_number = sp->cnt_task_init;
    /* record starting ib and offset */
    t->in_buf = ib->in_buf;
    /* if the size is 0, this empty task indicates EOF */
    t->in_buf_bytes = ib->in_buf_bytes;
    t->curr_rec = curr_rec;
    t->begin_rec = t->curr_rec;
    t->curr_in_buf_index = sp->cnt_in_buf_readable - 1;
    t->begin_in_buf_index = t->curr_in_buf_index;
    t->expected_end_index = -1;
    t->expected_end_offset = -1;
    t->first_in_buf = TRUE;
    t->outs_drained = 0;
    for (i = 0; i < sp->num_outputs; i++)
    {
        t->out[i].bytes_copied = 0;
        t->out[i].stalled = FALSE;
    }
    t->input_eof = FALSE;
    t->output_eof = FALSE;
    sp->cnt_task_init++;
    return (t);
}


/* new_in_buf - wait, if necessary, until an in_buf is available to be filled
 *              with input data.
 */
static void new_in_buf(sp_t sp)
{
    in_buf_t            *ib = NULL;

    TRACE("new_in_buf: waiting for buffer\n"); 
    pthread_mutex_lock(&sp->sump_mtx);
    while (sp->error_code == 0 &&
           sp->cnt_in_buf_readable >= sp->cnt_in_buf_done + sp->num_in_bufs)
    {
        /* get oldest buffer not yet recognized as done */
        ib = &sp->in_buf[sp->cnt_in_buf_done % sp->num_in_bufs];
        /* if all readers of this input buffer are done reading */
        if (ib->num_readers == ib->num_readers_done)
        {
            sp->cnt_in_buf_done++;
            break;
        }
        pthread_cond_wait(&sp->in_buf_done_cond, &sp->sump_mtx);
    }
    pthread_mutex_unlock(&sp->sump_mtx);
}


/* eof_without_new_in_buf_or_task - clean eof, no need to flush an input
 *                                  buffer or start a new task.
 */
static void eof_without_new_in_buf_or_task(sp_t sp)
{
    pthread_mutex_lock(&sp->sump_mtx);
    sp->input_eof = TRUE;
    /* wake sump thread waiting for next input buffer (just signal OK?) */
    pthread_cond_broadcast(&sp->in_buf_readable_cond);
    /* wake all sump threads waiting for new task */
    pthread_cond_broadcast(&sp->task_avail_cond);
    /* wake writer thread as it should exit on EOF */
    pthread_cond_broadcast(&sp->task_output_ready_cond); 
    pthread_mutex_unlock(&sp->sump_mtx);
}


/* flush_in_buf - flush an input buffer and start a new task if necessary
 */
static void flush_in_buf(sp_t sp, size_t buf_bytes, int eof)
{
    in_buf_t            *ib;
    sp_task_t           t;
    char                *curr_rec;
    char                *p;
        
    /* get ready to release in_buf to pump threads executing pump funcs */
    /* initially, no readers (there will be at least one) */
    ib = &sp->in_buf[sp->cnt_in_buf_readable % sp->num_in_bufs];
    ib->num_readers = 0;     
    ib->num_readers_done = 0;
    curr_rec = ib->in_buf;
    ib->in_buf_bytes = buf_bytes;
    sp->in_buf_current_bytes = 0;

    /* if this is not the first buffer and we are not processing
     * whole buffers.
     */
    if (sp->cnt_in_buf_readable != 0 && REC_TYPE(sp) != SP_WHOLE_BUF)
    {
        /* if we are grouping record by key values
         */
        if (sp->flags & SP_GROUP_BY)
        {
            /* the pump thread performing the most previously
             * issued task will always read this input buffer in
             * order to find the end of its input
             */
            ib->num_readers = 1;

            switch (REC_TYPE(sp))
            {
              case SP_UTF_8:
                /* scan until either we find a record whose key
                 * difference index is less than the number of
                 * "group by" keys, or we scan to the end of the buffer.
                 */
                while (curr_rec < ib->in_buf + ib->in_buf_bytes)
                {
                    /* if this is not the beginning of the buffer or
                     * the buffer begins with a whole record.
                     */
                    if (curr_rec != ib->in_buf ||
                        sp->prev_in_buf_ending_rec_partial_bytes == 0)
                    {
                        /* if match character indicates a new key
                         * grouping, then stop as this the boundry
                         * point between tasks.
                         */
                        if (*curr_rec == '0')
                            break;
                    }
                    /* find next instance of newline in buffer, if any */
                    curr_rec = memchr(curr_rec, *(char *)sp->delimiter,
                                      ib->in_buf_bytes -
                                      (curr_rec - ib->in_buf));
                    if (curr_rec == NULL)
                    {
                        curr_rec = ib->in_buf + ib->in_buf_bytes;
                        break;
                    }
                    curr_rec++;     /* step over newline */
                }
                break;

              case SP_FIXED:
                if (sp->prev_in_buf_ending_rec_partial_bytes == 0)
                    curr_rec = ib->in_buf;
                else
                    curr_rec = ib->in_buf + (sp->rec_size + 1) -
                        sp->prev_in_buf_ending_rec_partial_bytes;
                while (curr_rec < ib->in_buf + ib->in_buf_bytes)
                {
                    if (*curr_rec == '0')
                        break;
                    curr_rec += sp->rec_size + 1;
                }
                break;
            }
        }
        else   /* we are not grouping records by key value */
        {
            /* if this in buffer starts with a partial record, then
             * the pump thread performing the previous issued task
             * will read this buffer in order to find the end of its
             * input
             */
            if (sp->prev_in_buf_ending_rec_partial_bytes != 0)
            {
                /* the previously issued task is required to read the
                 * record reamainder at the beginning of this buffer.
                 */
                ib->num_readers = 1;

                switch (REC_TYPE(sp))
                {
                  case SP_UTF_8:
                    /* find next instance of newline in buffer, if any */
                    curr_rec = memchr(curr_rec, *(char *)sp->delimiter,
                                      ib->in_buf_bytes -
                                      (curr_rec - ib->in_buf));
                    if (curr_rec == NULL)
                        curr_rec = ib->in_buf + ib->in_buf_bytes;
                    else
                        curr_rec++;     /* step over newline */
                    break;

                  case SP_FIXED:
                    curr_rec = ib->in_buf + (sp->rec_size -
                                             sp->prev_in_buf_ending_rec_partial_bytes);
                    break;
                }            
            }
        }
    }

    switch (REC_TYPE(sp))
    {
      case SP_UTF_8:
        /* determine how many bytes the are in any partial record at
         * the end of this buffer.  search backwards to find last newline
         * character.
         */
        for (p = ib->in_buf + ib->in_buf_bytes - 1; p >= ib->in_buf; p--)
            if (*p == *(char *)sp->delimiter) 
                break;
        if (p < ib->in_buf)      /* if there was no newline */
        {
            /* add entire buffer size to this partial record */
            sp->prev_in_buf_ending_rec_partial_bytes += ib->in_buf_bytes;
        }
        else
            sp->prev_in_buf_ending_rec_partial_bytes =
                (ib->in_buf + ib->in_buf_bytes - 1) - p;
        break;

      case SP_FIXED:
        sp->prev_in_buf_ending_rec_partial_bytes =
            (sp->prev_in_buf_ending_rec_partial_bytes + ib->in_buf_bytes) %
            (sp->rec_size + ((sp->flags & SP_GROUP_BY) ? 1 : 0));
        break;

      case SP_WHOLE_BUF:
        sp->prev_in_buf_ending_rec_partial_bytes = 0;
        curr_rec = ib->in_buf;
        break;
    }
        
    TRACE("flush_in_buf: ib %d readable with %d bytes\n",
          sp->cnt_in_buf_readable, ib->in_buf_bytes);
    pthread_mutex_lock(&sp->sump_mtx);
    /* make input buffer available to any existing task.
     * in theory there should be only one task waiting */
    sp->cnt_in_buf_readable++;
    pthread_cond_broadcast(&sp->in_buf_readable_cond);

    if (eof)
    {
        /* if we found the starting point for a new task, then add a
         * reader for the task that will start its input with this
         * input buffer.
         */
        if (curr_rec < ib->in_buf + ib->in_buf_bytes)
            ib->num_readers++;

        /* set the expected end point for the previous task as the
         * actual begin point for the task we are about to define.
         */
        if (sp->cnt_task_init != 0) /* if there was a previous task */
        {
            t = &sp->task[(sp->cnt_task_init - 1) % sp->num_tasks];

            /* if we just made availible a non-empty in_buf
             * that did NOT start a new task.
             */
            if (ib->in_buf_bytes != 0 &&
                curr_rec >= ib->in_buf + ib->in_buf_bytes)
            {
                /* the expected ending in_buf is the next (and empty)
                 * one to be issued.
                 */
                t->expected_end_index = sp->cnt_in_buf_readable;
                t->expected_end_offset = 0;
                pthread_mutex_unlock(&sp->sump_mtx);
                /* do not issue a new task here. */
                eof_without_new_in_buf_or_task(sp);
                return;
            }
            else
            {
                /* the expected ending in_buf is the one just issued
                 * and the offset is the end.
                 */
                t->expected_end_index = sp->cnt_in_buf_readable - 1;
                t->expected_end_offset = (int)(curr_rec - ib->in_buf);
            }

            /* Note: it is possible that at this point the previous
             * task has already completed.  This is why tasks should
             * not use their expected ending point to confirm that
             * they ended at the right point.  This thread should
             * perform the check after the task has completed. */
        }

        /* make sure there is at least one available task struct */
        check_task_done(sp);
        if (sp->error_code != 0)
        {
            pthread_mutex_unlock(&sp->sump_mtx);
            return;
        }

        TRACE("flush_in_buf: initializing task %d\n", sp->cnt_task_init);
        t = init_new_task(sp, ib, curr_rec);
        sp->input_eof = TRUE;
        /* wake all sump threads */
        pthread_cond_broadcast(&sp->task_avail_cond);
        /* wake writer thread as it should exit on EOF */
        pthread_cond_broadcast(&sp->task_output_ready_cond); 
        pthread_mutex_unlock(&sp->sump_mtx);
        return;
    }

    /* if we found the ending point for a task (or this was the
     * very first input read), then start a new task.
     */
    if (curr_rec < ib->in_buf + ib->in_buf_bytes)
    {
        /* add a reader for the task that will start its input with
         * this input buffer.
         */
        ib->num_readers++;

        /* set the expected end point for the previous task as the
         * actual begin point for the task we are about to define.
         */
        if (sp->cnt_task_init != 0) /* if there was a previous task */
        {
            t = &sp->task[(sp->cnt_task_init - 1) % sp->num_tasks];

            /* the expected ending in_buf is the one just issued
             * and the offset is.
             */
            t->expected_end_index = sp->cnt_in_buf_readable - 1;
            t->expected_end_offset = (int)(curr_rec - ib->in_buf);

            /* Note: it is possible that at this point the previous
             * task has already completed.  This is why tasks should
             * not use their expected ending point to confirm that
             * they ended at the right point.  This thread should
             * perform the check after the task has completed. */
        }

        /* make sure there is at least one available task struct */
        check_task_done(sp);
        if (sp->error_code != 0)
        {
            pthread_mutex_unlock(&sp->sump_mtx);
            return;
        }

        TRACE("flush_in_buf: initializing task %d\n", sp->cnt_task_init);
        t = init_new_task(sp, ib, curr_rec);

        /* wake 1 sump thread */
        pthread_cond_signal(&sp->task_avail_cond);
    }
    pthread_mutex_unlock(&sp->sump_mtx);
    return;
}


/* sp_write_input - write data that is the input to a sump pump.
 *                  A write size of 0 indicates input EOF.
 */
ssize_t sp_write_input(sp_t sp, void *buf, ssize_t size)
{
    size_t              src_remaining = size;
    size_t              dst_remaining;
    size_t              trans_size;
    char                *trans_src;
    char                *trans_dst;
    in_buf_t            *ib;
#if !defined(SUMP_PUMP_NO_SORT)
    int                 ret;
#endif
            
    TRACE("sp_write_input: size %d\n", size);
    
    if (size <= 0 && sp->input_eof)
        return (0);    /* ignore, already eof */
    if (sp->error_code)
        return (-1);   /* already error */

    if (size <= 0)
    {
        if (size < 0)
        {
            /* TO-DO: need to make sure a partial input record does not
             * cause a sump hang instead of a sump error.  A sump hang
             * shouldn't matter if sump pump invoker waits for sump
             * pumps in upstream-to-downstream order */
            sp->error_code = SP_UPSTREAM_ERROR;
            pthread_mutex_lock(&sp->sump_mtx);
            broadcast_all_conds(sp);
            pthread_mutex_unlock(&sp->sump_mtx);
            size = 0;   /* act as if normal eof */
#if !defined(SUMP_PUMP_NO_SORT)
            if (sp->flags & SP_SORT)
            {
                (*Nsort_end)(&sp->nsort_ctx);
            }
#endif
            return (0);  /* caller is indicating error, return OK status */
        }
        /* else size == 0 */
#if !defined(SUMP_PUMP_NO_SORT)
        if (sp->flags & SP_SORT)
        {
            ret = (*Nsort_release_end)(&sp->nsort_ctx);

            if (ret < 0)        /* if error */
            {
                post_nsort_error(sp, ret);
                return (-1);
            }
            else
            {
                pthread_mutex_lock(&sp->sump_mtx);
                sp->sort_state = SORT_OUTPUT;
                pthread_cond_broadcast(&sp->task_output_ready_cond);
                pthread_mutex_unlock(&sp->sump_mtx);
                return (0);
            }
        }
#endif
        /* for non-sort case, fall through */
    }

#if !defined(SUMP_PUMP_NO_SORT)
    if (sp->flags & SP_SORT)
    {
        if (sp->sort_state != SORT_INPUT)
            return (0);
        ret = (*Nsort_release_recs)(buf, size, &sp->nsort_ctx);
        if (ret < 0)        /* if error */
            post_nsort_error(sp, ret);
        return (ret == NSORT_SUCCESS ? size : 0);
    }
#endif

    /* if EOF and there isn't a partially filled input buffer needing release.
     */
    if (src_remaining == 0 && sp->in_buf_current_bytes == 0)
    {
        eof_without_new_in_buf_or_task(sp);
        return (0);
    }
    
    /* while this is not EOF or there is a partially filled input buffer
     */
    while (src_remaining != 0 || sp->in_buf_current_bytes != 0)
    {
        /* if it is NOT the case that we have already partially filled an
         * input buffer that has not yet been released to the sump pump.
         * then get a new input buffer.
         */
        if (sp->in_buf_current_bytes == 0)
        {
            new_in_buf(sp);
            if (sp->error_code != 0)
                return (-1);
        }

        ib = &sp->in_buf[sp->cnt_in_buf_readable % sp->num_in_bufs];
        TRACE("sp_write_input: readable: %d, partial: %d\n",
              sp->cnt_in_buf_readable, sp->in_buf_current_bytes);

        trans_src = (char *)buf + size - src_remaining;
        trans_size = src_remaining;
        trans_dst = ib->in_buf + sp->in_buf_current_bytes;
        dst_remaining = ib->in_buf_size - sp->in_buf_current_bytes;
        if (trans_size > dst_remaining)
            trans_size = dst_remaining;
        memcpy(trans_dst, trans_src, trans_size);
        src_remaining -= trans_size;
        sp->in_buf_current_bytes += trans_size;
        dst_remaining = ib->in_buf_size - sp->in_buf_current_bytes;

        /* if this isn't EOF and there is more space remaining the in_buf,
         * then return so caller can write more bytes or declare EOF.
         */
        if (size != 0 && dst_remaining > 0)
        {
            if (src_remaining != 0)
                die("sp_write_input: dst_remaining %d and src_remaining %d\n",
                    dst_remaining, src_remaining);
            TRACE("sp_write_input() returning %d, dst_remaining: %d\n",
                  size, dst_remaining);
            return size;
        }

        flush_in_buf(sp, sp->in_buf_current_bytes, size == 0);
    }

    if (sp->error_code)
        size = -1;
    TRACE("sp_write_input: returning %d\n", size);
    return (size);
}


/* sp_get_in_buf - get a pointer to an input buffer that an external
 *                    thread can fill with input data.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_get_in_buf(sp_t sp, uint64_t buf_index, void **buf, size_t *size)
{
    in_buf_t    *ib;
    
    if (sp->flags & SP_SORT)
        return (SP_SORT_INCOMPATIBLE);
    *buf = NULL;
    *size = 0;
    TRACE("sp_get_in_buf: waiting for input buffer\n");
    if (buf_index < sp->cnt_in_buf_readable ||
        buf_index >= sp->cnt_in_buf_readable + sp->num_in_bufs)
    {
        return (SP_BUF_INDEX_ERROR);
    }
    pthread_mutex_lock(&sp->sump_mtx);
    while (sp->error_code == 0 &&
           buf_index >= sp->cnt_in_buf_done + sp->num_in_bufs)
    {
        /* get oldest buffer not yet recognized as done */
        ib = &sp->in_buf[sp->cnt_in_buf_done % sp->num_in_bufs];
        /* if all readers of this input buffer are done reading */
        if (ib->num_readers == ib->num_readers_done)
        {
            sp->cnt_in_buf_done++;
            continue;
        }
        pthread_cond_wait(&sp->in_buf_done_cond, &sp->sump_mtx);
    }
    if (sp->error_code == 0)
    {
        ib = &sp->in_buf[buf_index % sp->num_in_bufs];
        *buf = ib->in_buf;
        *size = ib->in_buf_size;
    }
    pthread_mutex_unlock(&sp->sump_mtx);
    if (sp->error_code)
        return (sp->error_code);
    return (SP_OK);
}


/* sp_put_in_buf_bytes - flush bytes that have been placed in a sump
 *                          pump's input buffer by an external thread.
 *                          This function should only be used by first
 *                          calling sp_get_in_buf().
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_put_in_buf_bytes(sp_t sp, uint64_t buf_index, size_t size, int eof)
{
    if (sp->flags & SP_SORT)
        return (SP_SORT_INCOMPATIBLE);
    if (buf_index != sp->cnt_in_buf_readable)
        return (SP_BUF_INDEX_ERROR);
    if (size == 0)
        eof_without_new_in_buf_or_task(sp);
    else
        flush_in_buf(sp, size, eof);
    if (sp->error_code)
        return (sp->error_code);
    return (SP_OK);
}


/* sp_get_error - get the error code of a sump pump.
 *
 * Returns: SP_OK if no error has occurred, otherwise the error code.
 */
int sp_get_error(sp_t sp)
{
    return (sp->error_code);
}


/* pfunc_get_thread_index - can be used by pump functions to get the
 *                          index of the sump pump thread executing the
 *                          pump func.  For instance, if there are 4
 *                          sump pump threads, this function will return
 *                          0-3 depending on which of the 4 threads is
 *                          invoking it.
 *
 * Returns: The index of the requesting sump pump thread.
 */
int pfunc_get_thread_index(sp_task_t t)
{
    return (t->thread_index);
}


/* pfunc_get_task_number - can be used by pump functions to get the sump
 *                         pump task number being executed by the pump
 *                         function.  This number starts at 0 and
 *                         increases with each subsequent task
 *                         issued/started by the sump pump.
 *
 * Returns: The task number (starting with 0 as the first task) that the
 *          calling thread is currently executing.
 */
uint64_t pfunc_get_task_number(sp_task_t t)
{
    return (t->task_number);
}


/* pfunc_write - write function that can be used by a pump function to
 *               write the output data for the pump function.
 *
 * Returns: the number of bytes written.  If this is not the same as the
 *          requested size, an error has occurred.
 */
size_t pfunc_write(sp_task_t t, unsigned out_index, void *buf, size_t size)
{
    char        *src = (char *)buf;
    size_t      bytes_left = size;
    size_t      copy_bytes;
    sp_t        sp = t->sp;
    struct task_out *out = t->out + out_index;

    if (sp->error_code != SP_OK)
        return (0);
    
    if (out_index >= sp->num_outputs)
    {
        pfunc_error(t, "pfunc_write, out_index %d >= num_outputs %d\n",
                    out_index, sp->num_outputs);
        sp->error_code = SP_OUTPUT_INDEX_ERROR;
        return (0);
    }
    
    /* while can't fit existing map output buffer contents and the new record
     * into the map output buffer.
     */
    while (bytes_left + out->bytes_copied > out->size)
    {
        copy_bytes = out->size - out->bytes_copied;
        if (copy_bytes)
        {
            memmove(out->buf + out->bytes_copied, src, copy_bytes);
            src += copy_bytes;
            bytes_left -= copy_bytes;
            out->bytes_copied += copy_bytes;
        }
        TRACE("pfunc_write: waking output reader\n");
        pthread_mutex_lock(&sp->sump_mtx);
        out->stalled = TRUE;
        pthread_cond_broadcast(&sp->task_output_ready_cond);
        TRACE("pfunc_write: waiting for available output buffer\n");
        while (out->stalled && sp->error_code == 0)
            pthread_cond_wait(&sp->task_output_empty_cond, &sp->sump_mtx);
        pthread_mutex_unlock(&sp->sump_mtx);
        if (sp->error_code != 0)
            return (-1);
    }
    /* copy new record into buffer */
    memmove(out->buf + out->bytes_copied, src, bytes_left);
    out->bytes_copied += bytes_left;
    return (size);
}


/* pfunc_mutex_lock - lock the auto-allocated mutex for pump functions.
 */
void pfunc_mutex_lock(sp_task_t t)
{
    pthread_mutex_lock(&t->sp->sp_mtx);
}


/* pfunc_mutex_unlock - unlock the auto-allocated mutex for pump functions.
 */
void pfunc_mutex_unlock(sp_task_t t)
{
    pthread_mutex_unlock(&t->sp->sp_mtx);
}


/* done_reading_in_buf - internal routine to mark the task's current
 *                       input buffer as done for reading.  This is
 *                       non-trivial because multiple pump threads may
 *                       have to read the same input buffer.
 */
static void done_reading_in_buf(sp_task_t t, int move_to_next_in_buf)
{
    in_buf_t    *ib;
    sp_t        sp = t->sp;

    pthread_mutex_lock(&sp->sump_mtx);

    /* bump up the count of done readers for the input buffer */
    ib = &sp->in_buf[t->curr_in_buf_index % sp->num_in_bufs];
    ib->num_readers_done++;
    /* if all readers are now done, signal the reader thread */
    if (ib->num_readers == ib->num_readers_done)
        pthread_cond_broadcast(&sp->in_buf_done_cond);

    pthread_mutex_unlock(&sp->sump_mtx);

    if (move_to_next_in_buf)
    {
        /* move this task's current input position to the beginning of
         * the next input buffer.
         */
        t->curr_in_buf_index++;   /* on to next input buffer */
        ib = &sp->in_buf[t->curr_in_buf_index % sp->num_in_bufs];
        t->in_buf = ib->in_buf;
        t->in_buf_bytes = 0;
        t->curr_rec = t->in_buf;
    }
}


/* ready_in_buf - internal routine to insure the current input buffer
 *                for a task is ready for reading.
 */
static void ready_in_buf(sp_task_t t)
{
    in_buf_t     *ib;
    sp_t        sp = t->sp;

    pthread_mutex_lock(&sp->sump_mtx);

    t->first_in_buf = FALSE; /* now that this task has proceeded beyond
                              * its first input buffer, it should stop
                              * at end of the current key group */

    while (sp->error_code == 0 && !sp->input_eof && 
           t->curr_in_buf_index == sp->cnt_in_buf_readable) /*not yet readable*/
    {
        pthread_cond_wait(&sp->in_buf_readable_cond, &sp->sump_mtx);
    }
    
    /* if sp_write_input() has indicated eof and no more readable buffers
     * then that indicates eof for this task.
     */
    if (sp->error_code != 0 ||
        (sp->input_eof &&
         t->curr_in_buf_index == sp->cnt_in_buf_readable))
    {
        t->input_eof = TRUE;
        TRACE("pump%d: eof: error_code: %d, in_buf_bytes is 0 at ib %d\n",
              t->thread_index, sp->error_code, t->curr_in_buf_index);
        t->in_buf = NULL;
        t->in_buf_bytes = 0;
        t->curr_rec = NULL;
    }
    else
    {
        ib = &sp->in_buf[t->curr_in_buf_index % sp->num_in_bufs];
        t->in_buf = ib->in_buf;
        t->in_buf_bytes = ib->in_buf_bytes;        
        t->curr_rec = t->in_buf;
    }

    pthread_mutex_unlock(&sp->sump_mtx);
}


/* is_more_input - internal routine to test if there is more input for
 *                 this pump task.  This routine is called after the
 *                 pump function returns to see if there is additonal
 *                 input data for the task and hence the pump function
 *                 should be called again.
 */
static int is_more_input(sp_task_t t)
{
    if (t->input_eof)   /* if input eof, then false (no more input) */
        return (0);
    
    /* if we are past the first input buffer for this task (because we
     * had to read the remainder of the last task record at the
     * beginning of the second input buffer), then we have hit the end
     * of this task's input.  Note that if GROUP_BY is specified,
     * then the task pump function can read past any record remainder
     * at the beginning of the second input buffer.
     */
    if (!t->first_in_buf && !(t->sp->flags & SP_GROUP_BY))
    {
        done_reading_in_buf(t, t->curr_rec == t->in_buf + t->in_buf_bytes);
        t->input_eof = TRUE;
        return (0);
    }
    
    /* if there are bytes left in the current input buffer, then true
     * (more input).
     */
    TRACE("pump: imi, rec %08x, buf %08x, bytes: %x\n",
          t->curr_rec, t->in_buf, t->in_buf_bytes);
    if (t->curr_rec < t->in_buf + t->in_buf_bytes)
        return (1);

    /* this task is done reading its current input buffer */
    done_reading_in_buf(t, TRUE);
    
    /* if we are not grouping by key difference, then we have hit the
     * end of this task's input.
     */
    if (!(t->sp->flags & SP_GROUP_BY))
    {
        t->input_eof = TRUE;
        return (0);
    }

    /* get next input buffer */
    ready_in_buf(t);
    TRACE("pump: imi, eof %d\n", t->input_eof);
    return (!t->input_eof);
}


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
size_t pfunc_get_rec(sp_task_t t, void *ptr_to_rec_ptr)
{
    void        *buf;
    char        *rec;
    char        *next_rec;
    int         match_char;
    size_t      len;
    size_t      src_size;
    size_t      trans_size;
    size_t      delim_size = 0;
    sp_t        sp = t->sp;
    int         new_key_group_beginning = FALSE;

    buf = t->rec_buf;
    
    if (t->input_eof)
        return 0;

    if (REC_TYPE(sp) == SP_UTF_8)
        delim_size = 1;
    
    /* if we have used all the records in the current input buffer...
     * note that we must be on a record boundry at this point.
     */
    if (t->curr_rec >= t->in_buf + t->in_buf_bytes)
    {
        done_reading_in_buf(t, TRUE);  /* done reading input buffer */
        
        /* if we are not grouping records then return no record (0) since
         * we are on a record boundry.
         */
        if (!(sp->flags & SP_GROUP_BY))
        {
            t->input_eof = TRUE;
            return 0;
        }

        ready_in_buf(t);
        if (t->input_eof)
            return 0;
    }

    /* now we have at least a partial record in the input buffer */
    rec = t->curr_rec;
    /* if there is an initial key offset character */
    if (sp->flags & SP_GROUP_BY)
    {
        match_char = rec[0];
        if (match_char != '0' && match_char != '1')
        {
            pfunc_error(t, "-group was specified, but an input record does not start with\n"
                        "'0' or '1'. Was nsort -match used to generate input?\n");
            /* return 0; don't return EOF as this can result in infinite loop*/
        }
        rec++;
        new_key_group_beginning =
            (match_char == '0' && !t->first_group_rec);
    }

    /* if the key offset indicates this is the beginning of a new key group &&
     * this is not the actual first record for this current key group.
     */
    if (!(sp->flags & SP_GROUP_BY) || new_key_group_beginning)
    {
        /* if we are beyond the first input buffer for this task, this
         * rec is not only the beginning of the next key group, it also
         * means we have reached the end of the input for this sump pump
         * task.
         */
        if (!t->first_in_buf)
        {
            done_reading_in_buf(t, FALSE);
            TRACE("pump%d: eof: match: %c at ib %d, curr_rec %08x\n",
                  t->thread_index,
                  (sp->flags & SP_GROUP_BY) ? match_char : '0',
                  t->curr_in_buf_index, t->curr_rec);
            t->input_eof = TRUE;   /* no need to be inside the mutex because
                                     * only the current thread reads this */
            return 0;
        }
        /*
         * else we are still reading from the first input buffer for this
         * task, but there is at least one more record key group to be
         * processed by this task.
         */
        else if (new_key_group_beginning)
            return 0;
        /* else we are not doing key grouping, go on to return next rec */
    }
    t->first_group_rec = FALSE;  /* only relevant for key grouping */

    /* now copy the record, which may span multiple in_bufs, into the
     * caller's buffer.
     */
    len = 0;
    for (;;)
    {
        src_size = t->in_buf_bytes - (rec - t->in_buf);
        switch (REC_TYPE(sp))
        {
          case SP_UTF_8:
            next_rec = memchr(rec, *(char *)sp->delimiter, src_size);
            if (next_rec != NULL)
            {
                next_rec++;         /* skip over newline */
                trans_size = next_rec - rec;
            }
            else
                trans_size = src_size;
            break;

          case SP_FIXED:
            trans_size = src_size;
            if (trans_size >= sp->rec_size - len)
            {
                trans_size = sp->rec_size - len;
                next_rec = rec + trans_size;
            }
            else
                next_rec = NULL;
            break;
        }            
        if (trans_size + len + delim_size > t->rec_buf_size)
        {
            size_t new_size;

            if (REC_TYPE(sp) == SP_FIXED)
                new_size = sp->rec_size;
            else
                new_size = trans_size + len + delim_size + 50;
            
            if (t->rec_buf == NULL)
                t->rec_buf = (char *)calloc(1, new_size);
            else
                t->rec_buf = realloc(t->rec_buf, new_size);
            if (t->rec_buf == NULL)
            {
                die("pfunc_get_rec: rec_buf increase failed: old %d new %d\n",
                    t->rec_buf_size, new_size);
            }
            t->rec_buf_size = new_size;
            buf = t->rec_buf;
        }
        memmove((char *)buf + len, rec, trans_size);
        len += trans_size;
        if (next_rec != NULL)  /* if we found the newline delimiter */
        {
            if (REC_TYPE(sp) == SP_UTF_8)
                ((char *)buf)[len] = '\0';
            break;
        }
        else  /* else we need to get remainder of record from next buffer */
        {
            done_reading_in_buf(t, TRUE);
            ready_in_buf(t);
            if (t->input_eof)
            {
                if (REC_TYPE(sp) == SP_FIXED)
                {
                    die("pfunc_get_rec: partial record of "
                        "%d bytes found at end of input\n", len);
                }
                return 0;
            }
            rec = t->curr_rec;
        }
    }
    t->curr_rec = next_rec;
    *(void **)ptr_to_rec_ptr = buf;
    return len;
}


/* pfunc_get_in_buf - get a pointer to the input buffer for a pump function.
 */
int pfunc_get_in_buf(sp_task_t t, void **buf, size_t *size)
{
    *(char **)buf = t->curr_rec;
    *size = (t->in_buf + t->in_buf_bytes) - t->curr_rec;
    t->curr_rec = t->in_buf + t->in_buf_bytes;
    return (0);
}


/* pfunc_get_out_buf - get a pointer to an output buffer and its size for
 *                     a pump function.
 *
 * Returns: SP_OK or a sump pump error code
 */
int pfunc_get_out_buf(sp_task_t t, unsigned out_index, void **buf, size_t *size)
{
    sp_t                sp = t->sp;
    struct task_out     *out = t->out + out_index;

    if (out_index >= sp->num_outputs)
    {
        pfunc_error(t, "pfunc_get_out_buf: out_index %d >= num_outputs %d\n",
                    out_index, sp->num_outputs);
    }

    if (out->bytes_copied == out->size)
    {
        sp_t    sp = t->sp;
        
        TRACE("pfunc_get_out_buf: waking output reader\n");
        pthread_mutex_lock(&sp->sump_mtx);
        out->stalled = TRUE;
        pthread_cond_broadcast(&sp->task_output_ready_cond);
        TRACE("pfunc_get_out_buf: waiting for available output buffer\n");
        while (out->stalled && sp->error_code == 0)
            pthread_cond_wait(&sp->task_output_empty_cond, &sp->sump_mtx);
        pthread_mutex_unlock(&sp->sump_mtx);
        if (sp->error_code != 0)
            return (-1);
    }
    *(char **)buf = out->buf + out->bytes_copied;
    *size = out->size - out->bytes_copied;
    return (0);
}


/* pfunc_put_out_buf_bytes - flush bytes that have been placed in a pump
 *                           function's output buffer.  This routine can only
 *                           be used by first calling sp_get_out_buf().
 *
 * Returns: SP_OK or a sump pump error code
p */
int pfunc_put_out_buf_bytes(sp_task_t t, unsigned out_index, size_t size)
{
    sp_t                sp = t->sp;
    struct task_out     *out = t->out + out_index;

    if (out_index >= sp->num_outputs)
    {
        pfunc_error(t, "pfunc_put_out_buf_bytes: "
                    "out_index %d >= num_outputs %d\n",
                    out_index, sp->num_outputs);
    }
    else if (out->bytes_copied + size > out->size)
    {
        pfunc_error(t, "sp_put_out_buf_bytes: "
                    "aggregate size (%d+%d) is larger than buf size %d\n",
                    out->bytes_copied, size, out->size);
    }
    else
        out->bytes_copied += size;
    return (sp->error_code);
}


/* pfunc_printf - print a formatted string to a pump functions's output.
 *
 * Returns: the number of bytes written.  If this is not the same as the
 *          requested size, an error has occurred.
 */
int pfunc_printf(sp_task_t t, unsigned out_index, const char *fmt, ...)
{
    va_list     ap;
    ssize_t     ret;

    va_start(ap, fmt);
    ret = vsnprintf(t->temp_buf, t->temp_buf_size, fmt, ap);
    va_end(ap);
#if defined(win_nt)
    if (ret == -1)  /* non-standard vsnprintf overflow indicator on Windows */
    {
        va_start(ap, fmt);
        ret = _vscprintf(fmt, ap);
        va_end(ap);
    }
#endif
    if ((size_t)ret >= t->temp_buf_size)
    {
        /* temp buf wasn't big enough.  enlarge it and redo */
        if (t->temp_buf_size != 0)
            free(t->temp_buf);
        t->temp_buf_size = ret + 10;
        t->temp_buf = (char *)malloc(t->temp_buf_size);
        va_start(ap, fmt);
        ret = vsnprintf(t->temp_buf, t->temp_buf_size, fmt, ap);
        va_end(ap);
    }
    return ((int)pfunc_write(t, out_index, t->temp_buf, ret));
}


/* pfunc_error - raise an error for a pump function and define an error 
 *               string that can be retrived by other threads using
 *               sp_get_error_string().
 *
 * Returns: SP_PUMP_FUNCTION_ERROR
 */
int pfunc_error(sp_task_t t, const char *fmt, ...)
{
    va_list     ap;
    int         ret;

    if (t->error_code != 0)            /* if prior error */
        return SP_PUMP_FUNCTION_ERROR;   /* ignore this one */
    t->error_code = SP_PUMP_FUNCTION_ERROR;
    
    va_start(ap, fmt);
    ret = vsnprintf(t->error_buf, t->error_buf_size, fmt, ap);
    va_end(ap);
#if defined(win_nt)
    if (ret == -1)  /* non-standard vsnprintf overflow indicator on Windows */
    {
        va_start(ap, fmt);
        ret = _vscprintf(fmt, ap);
        va_end(ap);
    }
#endif
    if ((size_t)ret >= t->error_buf_size)
    {
        if (t->error_buf_size != 0)
            free(t->error_buf);
        t->error_buf_size = (size_t)ret + 1;
        t->error_buf = (char *)malloc(t->error_buf_size);
        va_start(ap, fmt);
        vsnprintf(t->error_buf, t->error_buf_size, fmt, ap);
        va_end(ap);
    }
    
    return SP_PUMP_FUNCTION_ERROR;
}


/* pump_thread_main - the internal "main" routine of a sump pump thread.
 */
static void *pump_thread_main(void *arg)
{
    sp_task_t           t;
    unsigned            thread_index;
    sp_t                sp = (sp_t)arg;
    int                 ret;

    for (thread_index = 0; thread_index < sp->num_threads; thread_index++)
#if defined(win_nt)
        if (sp->thread[thread_index].id == GetCurrentThreadId())
#else
        if (sp->thread[thread_index] == pthread_self())
#endif
            break;
    TRACE("pump thread %d starting\n", thread_index);
    for (;;)
    {
        TRACE("pump%d: waiting for an available task\n", thread_index);
        pthread_mutex_lock(&sp->sump_mtx);
        while (sp->cnt_task_begun == sp->cnt_task_init &&
               sp->error_code == 0 &&
               sp->input_eof == FALSE)
        {
            pthread_cond_wait(&sp->task_avail_cond, &sp->sump_mtx);
        }
        if (sp->error_code != 0 ||
            (sp->input_eof == TRUE && sp->cnt_task_begun == sp->cnt_task_init))
        {
            pthread_mutex_unlock(&sp->sump_mtx);
            TRACE("pump%d: breaking out of for loop: error_code: %d, input_eof %d\n",
                  thread_index, sp->error_code, sp->input_eof);
            break;
        }
        t = &sp->task[sp->cnt_task_begun % sp->num_tasks];
        sp->cnt_task_begun++;
        t->thread_index = thread_index;
        pthread_mutex_unlock(&sp->sump_mtx);

        TRACE("pump%d: calling pump func with %d input bytes\n",
              thread_index, (int)t->in_buf_bytes);

        if (REC_TYPE(sp) == SP_WHOLE_BUF)
        {
            TRACE("pump%d: calling pump func() block\n", thread_index);
            ret = (*sp->pump_func)(t, sp->pump_arg);
            TRACE("pump%d: pump func returned %d\n", thread_index, ret);
            if (ret)
            {
                if (t->error_code == 0)
                    t->error_code = ret;
            }
            else
            {
                done_reading_in_buf(t, TRUE);
                t->input_eof = TRUE;
            }
        }
        else
        {
            while (is_more_input(t) && t->error_code == 0)
            {
                TRACE("pump%d: calling pump func()\n", thread_index);
                /* indicate first record in key group not yet read */
                t->first_group_rec = TRUE;
                ret = (*sp->pump_func)(t, sp->pump_arg);
                TRACE("pump%d: pump func returned %d, input_eof: %d\n",
                      thread_index, ret, t->input_eof);
                if (ret && t->error_code == 0)
                    t->error_code = ret;
            }
        }
        TRACE("pump%d: pump_func returns with %d out[0] bytes\n",
              thread_index, t->out[0].bytes_copied);
        pthread_mutex_lock(&sp->sump_mtx);
        if (t->error_code && sp->error_code == 0)
        {
            sp->error_code = t->error_code;
            if (sp->error_buf != NULL)
                free(sp->error_buf);
            sp->error_buf = t->error_buf;
            t->error_buf = NULL;
            broadcast_all_conds(sp);
        }
        pthread_mutex_unlock(&sp->sump_mtx);

        TRACE("pump%d: waking output reader\n", thread_index);
        TRACE("pump%d: waking input writer\n", thread_index);
        pthread_mutex_lock(&sp->sump_mtx);
        t->output_eof = TRUE;
        pthread_cond_broadcast(&sp->task_output_ready_cond);
        pthread_mutex_unlock(&sp->sump_mtx);
        /* NOTA BENE: do not use "t" pointer after this point since the
         * struct that it points to can be reused immediately */
    }
    TRACE("pump%d: exiting\n", thread_index);

    return (NULL);
}


/* sp_read_output - read bytes from the specified output of a sump pump.
 *
 * Returns: The number of bytes read.  If 0, then EOF has occurred.
 *          If negative, an error has occurred.
 */
ssize_t sp_read_output(sp_t sp, unsigned index, void *buf, ssize_t size)
{
    int                 out_eof;
    ssize_t             bytes_returned = 0;
    ssize_t             src_remaining = size;
    ssize_t             dst_remaining;
    ssize_t             trans_size;
    char                *trans_src;
    char                *trans_dst;
    sp_task_t           t;
    struct task_out     *out;

    TRACE("sp_read_output[%d]: buf %08x, size %d\n", index, buf, size);

    if (sp->error_code)
        return (-1);

#if !defined(SUMP_PUMP_NO_SORT)
    if (sp->flags & SP_SORT)
    {
        int     ret;

        if (sp->sort_state == SORT_INPUT)
        {
            pthread_mutex_lock(&sp->sump_mtx);
            while (sp->error_code == 0 && sp->sort_state == SORT_INPUT)
                pthread_cond_wait(&sp->task_output_ready_cond, &sp->sump_mtx);
            pthread_mutex_unlock(&sp->sump_mtx);
            if (sp->error_code != 0)
                return (-1);
        }
        if (sp->sort_state == SORT_DONE)
            return (0);

        while (bytes_returned < size)
        {
            /* if there is no data in the sort_temp_buf, then fill it.
             */
            if (sp->out[0].partial_bytes_copied == 0)
            {
                /* call nsort_return_recs() until it does NOT return a buffer
                 * too small error.
                 */
                for (;;)
                {
                    char    *temp;
                
                    sp->sort_temp_buf_bytes = sp->sort_temp_buf_size;
                    ret = (*Nsort_return_recs)(sp->sort_temp_buf,
                                               &sp->sort_temp_buf_bytes,
                                               &sp->nsort_ctx);
                    if (ret != NSORT_RETURN_BUF_SMALL)
                        break;
                    temp = sp->sort_temp_buf;
                    sp->sort_temp_buf_size *= 2;
                    sp->sort_temp_buf = (char *)malloc(sp->sort_temp_buf_size);
                    if (sp->sort_temp_buf == NULL)
                    {
                        sp->error_code = SP_MEM_ALLOC_ERROR;
                        return (-1);
                    }
                    free(temp);
                }
                if (ret == NSORT_END_OF_OUTPUT)
                {
                    pthread_mutex_lock(&sp->sump_mtx);
                    sp->sort_state = SORT_DONE;
                    pthread_cond_broadcast(&sp->task_output_ready_cond);
                    pthread_mutex_unlock(&sp->sump_mtx);
                    return (bytes_returned);
                }
                else if (ret < 0)
                {
                    post_nsort_error(sp, ret);
                    return (-1);
                }
            }

            src_remaining =
                sp->sort_temp_buf_bytes - sp->out[0].partial_bytes_copied;
            dst_remaining = size - bytes_returned;
            trans_size = dst_remaining;
            trans_src = sp->sort_temp_buf + sp->out[0].partial_bytes_copied;
            trans_dst = (char *)buf + bytes_returned;
            if (dst_remaining <= src_remaining)
            {
                /* we will fill the caller's buffer completely.
                 */
                memmove(trans_dst, trans_src, dst_remaining);
                bytes_returned += dst_remaining;
                sp->out[0].partial_bytes_copied += dst_remaining;
                break;
            }

            /* the sort temp buffer will be completely copied out,
             * and we still need more.
             */
            memmove(trans_dst, trans_src, src_remaining);
            bytes_returned += src_remaining;
            /* indicate sort temp output buffer needes to be refilled */
            sp->out[0].partial_bytes_copied = 0;
        }
        return (bytes_returned);
    }
#endif
    
    if (index >= sp->num_outputs)
        return (-1);
            
    for (;;)
    {
        TRACE("sp_read_output: out[%d].cnt_task_drained: %d, "
              "partial_bytes_copied: %d\n",
              index, sp->out[index].cnt_task_drained,
              sp->out[index].partial_bytes_copied);
        t = &sp->task[sp->out[index].cnt_task_drained % sp->num_tasks];
        out = t->out + index;

        /* if we aren't in the middle of copy out a task's buffer, then
         * we must potentially wait for the output of the next task.
         */
        if (sp->out[index].partial_bytes_copied == 0)
        {
            TRACE("sp_read_output: waiting for input\n");
            pthread_mutex_lock(&sp->sump_mtx);

            /* while 1) a sump pump error hasn't occurred.
             * and   2) it's not the case that
             *          a) EOF on input has been reached
             *          b) all initialized tasks have begun (been taken), and
             *          c) all taken tasks have had their output read, and
             * and   3) it's not the case oldest sump pump task is either
             *          done or stalled
             */
            while (sp->error_code == 0 &&
                   !(out_eof = (sp->input_eof &&
                                sp->cnt_task_init == sp->cnt_task_begun &&
                                sp->cnt_task_begun == sp->out[index].cnt_task_drained)) &&
                   !(sp->out[index].cnt_task_drained < sp->cnt_task_begun &&
                     (out->stalled || t->output_eof)))
            {
                TRACE("sp_read_output: waiting\n");
                TRACE("sp_read_output: cnt_task_init: %d\n",
                      sp->cnt_task_init);
                TRACE("sp_read_output: cnt_task_begun: %d\n",
                      sp->cnt_task_begun);
                TRACE("sp_read_output: out[%d].cnt_task_drained: %d\n",
                      index, sp->out[index].cnt_task_drained);
                TRACE("sp_read_output: out->bytes_copied: %d\n",
                      out->bytes_copied);
                TRACE("sp_read_output: t->in_buf_bytes: %d\n",
                      t->in_buf_bytes);
                TRACE("sp_read_output: out->stalled: %d\n", out->stalled);
                TRACE("sp_read_output: t->output_eof: %d\n", t->output_eof);
                pthread_cond_wait(&sp->task_output_ready_cond, &sp->sump_mtx);
            }
            TRACE("sp_read_output: DONE WAITING\n");
            TRACE("sp_read_output: error_code: %d\n", sp->error_code);
            TRACE("sp_read_output: cnt_task_init: %d\n", sp->cnt_task_init);
            TRACE("sp_read_output: cnt_task_begun: %d\n", sp->cnt_task_begun);
            TRACE("sp_read_output: out[%d].cnt_task_drained: %d\n",
                  index, sp->out[index].cnt_task_drained);
            TRACE("sp_read_output: out->bytes_copied: %d\n",
                  out->bytes_copied);
            TRACE("sp_read_output: t->in_buf_bytes: %d\n", t->in_buf_bytes);
            TRACE("sp_read_output: out->stalled: %d\n", out->stalled);
            TRACE("sp_read_output: t->output_eof: %d\n", t->output_eof);
            TRACE("sp_read_output: out_eof: %d\n", out_eof);
            pthread_mutex_unlock(&sp->sump_mtx);
            if (sp->error_code || out_eof)
                break;
        }  
        src_remaining =
            out->bytes_copied - sp->out[index].partial_bytes_copied;
        dst_remaining = size - bytes_returned;
        trans_size = dst_remaining;
        trans_src = t->out[index].buf + sp->out[index].partial_bytes_copied;
        trans_dst = (char *)buf + bytes_returned;
        if (dst_remaining < src_remaining)
        {
            /* we will fill the output buffer and still leave some bytes
             * in the sump pump task's output buffer.
             */
            memmove(trans_dst, trans_src, dst_remaining);
            bytes_returned += dst_remaining;
            sp->out[index].partial_bytes_copied += dst_remaining;
            break;
        }

        /* the sump pump task's output buffer will be completely copied out.
         */
        memmove(trans_dst, trans_src, src_remaining);
        bytes_returned += src_remaining;
        /* indicate new sump pump task output is needed */
        sp->out[index].partial_bytes_copied = 0;  
        
        TRACE("sp_read_output: waking reader thread\n");
        pthread_mutex_lock(&sp->sump_mtx);
        if (t->out[index].stalled)
        {
            /* we have copied the bytes in the buf.  clear the buf
             * and stall indicator, then wake stalled thread.  since
             * there may be more than one thread waiting on the
             * task_output_empty_cond, use broadcast.
             */
            t->out[index].bytes_copied = 0;
            t->out[index].stalled = FALSE;
            pthread_cond_broadcast(&sp->task_output_empty_cond);
        }
        else
        {
            /* increment per-output task output drained count */
            sp->out[index].cnt_task_drained++;
            TRACE("sp_read_output: sp->out[%d].cnt_task_drained incr to %d\n",
                  index, sp->out[index].cnt_task_drained);
            TRACE("sp_read_output: t->outs_drained before incr is: %d\n",
                  t->outs_drained);

            /* if all output buffers for this task have been drained */
            if (++t->outs_drained == sp->num_outputs)  
            {
                /* increment sump pump task drained count */
                sp->cnt_task_drained++;
                TRACE("sp_read_output: sp->cnt_task_drained incr to: %d\n",
                      sp->cnt_task_drained);
                pthread_cond_broadcast(&sp->task_drained_cond);
            }
        }

        pthread_mutex_unlock(&sp->sump_mtx);

        if (bytes_returned == size)
            break;
    }

    if (sp->error_code)
        bytes_returned = -1;
    TRACE("sp_read_output: returning %d bytes\n", bytes_returned);
    return (bytes_returned);
}


/* link_main - internal "main" routine for a thread that links an output
 *             of a sump pump to the input of another sump pump.
 */
static void *link_main(void *arg)
{
    sp_link_t   sp_link = (sp_link_t)arg;
    ssize_t     size;

    while ((size = sp_read_output(sp_link->out_sp,
                                  sp_link->out_index,
                                  sp_link->buf,
                                  sp_link->buf_size)) > 0)
    {
        if (sp_write_input(sp_link->in_sp, sp_link->buf, size) != size)
        {
            TRACE("link_main: sp_write_input() returned wrong size\n");
            sp_link->error_code = SP_WRITE_ERROR;
            return (NULL);
        }
    }
    sp_write_input(sp_link->in_sp, NULL, size);/* need to handle err case?*/
    if (size < 0)
        sp_link->error_code = SP_UPSTREAM_ERROR;
    return (NULL);
}


/* sp_link - start a link/connection between an output
 *           of a sump pump to the input of another sump pump.
 *
 * Returns: SP_OK or a sump pump error code
 */
int sp_link(sp_t out_sp, unsigned out_index, sp_t in_sp)
{
    struct sp_link      *sp_link;
    int                 ret;
    int                 group_by_input;

    sp_link = (struct sp_link *)calloc(1, sizeof(struct sp_link));
    if (sp_link == NULL)
        return (SP_MEM_ALLOC_ERROR);
    group_by_input = (in_sp->flags & SP_GROUP_BY) ? TRUE : FALSE;
    if (out_sp->match_keys ^ group_by_input)
        return (SP_GROUP_BY_MISMATCH);
    sp_link->out_sp = out_sp;
    sp_link->out_index = out_index;
    sp_link->in_sp = in_sp;
    sp_link->buf_size = 4096;
    sp_link->buf = (char *)malloc(sp_link->buf_size);
    if (sp_link->buf == NULL)
        return (SP_MEM_ALLOC_ERROR);
    if ((ret = pthread_create(&sp_link->thread, NULL, link_main, sp_link)))
        die("sp_start_link: pthread_create() ret: %d\n", ret);

    return (SP_OK);
}


/* get_output_index - internal routine to read an output index preceded by
 *                    a "[" and followed by "]=".
 */
static int get_output_index(sp_t sp, char **caller_p)
{
    int         index;
    char        *p = *caller_p;
    
    if (*p++ != '[')
    {
        syntax_error(sp, p, "expected '['");
        return 0;
    }
    index = (int)get_numeric_arg(sp, &p);
    if (index < 0 || (unsigned)index >= sp->num_outputs)
    {
        syntax_error(sp, p,
                     "output index is greater than the number of outputs");
        return 0;
    }
    if (*p++ != ']')
    {
        syntax_error(sp, p, "expected ']'");
        return 0;
    }
    if (*p++ != '=')
    {
        syntax_error(sp, p, "expected '='");
        return 0;
    }
    *caller_p = p;
    return (index);
}


/* get_logical_processor_count - internal routine to get the number of
 *                               logical processors in the system.
 */
static int get_logical_processor_count()
{
#if defined(win_nt)
    {
        SYSTEM_INFO si;

        GetSystemInfo(&si);
        return si.dwNumberOfProcessors;
    }
#else
    return sysconf(_SC_NPROCESSORS_ONLN);
#endif
}


/* get_string_arg - internal routine to scan and return a string
 */
static char *get_string_arg(char **caller_p)
{
    char        *begin_p = *caller_p;
    char        *p;
    char        *ret;
    int         i;

    /* scan string up to the next white space character */
    p = begin_p;
    while (!isspace(*(unsigned char *)p) && *p != '\0')
        p++;
    *caller_p = p;

    /* allocate space for the return string and copy contents to it */
    ret = (char *)calloc(1, p - begin_p + 1);
    for (i = 0; i < p - begin_p; i++)
        ret[i] = begin_p[i];
    ret[p - begin_p] = '\0';
    return (ret);
}


/* sp_argv_to_str - bundle up the specified argv and return it as a string.
 *                  For instance if argc is 2, argv[0] is "TASKS=2" and
 *                  argv[1] is "THEADS=3", then return the string
 *                  "TASKS=2 THREADS=3".
 *
 * Returns: a string containing the command line arguments passed as function
 *          arguments. The string should be free()'d when no longer needed.
 */
char *sp_argv_to_str(char *argv[], int argc)
{
    int                 i;
    char                *str = NULL;
    int                 n_chars;

    n_chars = 1;      /* '\0' */
    for (i = 0; i < argc; i++)
        n_chars += 1 + (int)strlen(argv[i]);   /* ' ' + argv[i] */
    str = (char *)calloc(sizeof(char), n_chars + 1);
    strcpy(str, argc == 0 ? "" : argv[0]);
    for (i = 1; i < argc; i++)
    {
        strcat(str, "\n");
        strcat(str, argv[i]);
    }
    return (str);
}


/* get_exec_args - internal routine to get an external program name and its
 *                 arguments
 */
static void get_exec_args(sp_t sp, char **ep)
{
    char        *p, *begin;
    int         i, cnt;
    
#if defined(win_nt)
    int         cmdlen;
    char        *cmdline;
    
    for (p = *ep, cnt = 0; *p != '\0'; p++)
        if (*p == '\n')
            cnt++;      /* count newlines */
    sp->exec_argv = (char **)calloc(2, sizeof(char *));
    if (sp->exec_argv == NULL)
    {
        sp->error_code = SP_MEM_ALLOC_ERROR;
        return;
    }
    /* allocate space for each character plus 2 double-quote chars per arg */
    cmdline = sp->exec_argv[0] =
        (char *)calloc((p - *ep) + 2 * (cnt + 1), sizeof(char));
    if (cmdline == NULL)
    {
        sp->error_code = SP_MEM_ALLOC_ERROR;
        return;
    }
    cmdlen = 0;
#endif
    
    p = *ep;
    i = 0;
    cnt = 0;
    for (;;)
    {
#if defined(win_nt)
        int     need_quotes = FALSE;
        if (i != 0)
            cmdline[cmdlen++] = ' ';   /* add space between args */
#else           /* grow argv for non-Windows systems */
# define SP_ARGV_INCR    5
        if (i == 0)
        {
            cnt = SP_ARGV_INCR;
            sp->exec_argv = (char **)calloc(cnt + 1, sizeof(char *));
        }
        else if (i == cnt)
        {
            cnt += SP_ARGV_INCR;
            sp->exec_argv = (char **)
                realloc(sp->exec_argv, (cnt + 1) * sizeof(char *));
        }
        if (sp->exec_argv == NULL)
        {
            sp->error_code = SP_MEM_ALLOC_ERROR;
            return;
        }
#endif
        begin = p;
        while (*p != '\n' && *p != '\0')
        {
#if defined(win_nt)
            if (*p == ' ')
                need_quotes = TRUE;
#endif
            p++;
        }

#if defined(win_nt)
        if (need_quotes)
            cmdline[cmdlen++] = '"';   /* begin double-quote */
        memcpy(cmdline + cmdlen, begin, p - begin);
        cmdlen += p - begin;
        if (need_quotes)
            cmdline[cmdlen++] = '"';   /* end double-quote */
#else     
        sp->exec_argv[i] = calloc(sizeof(char), p - begin + 1);
        if (sp->exec_argv[i] == NULL)
        {
            sp->error_code = SP_MEM_ALLOC_ERROR;
            return;
        }
        memcpy(sp->exec_argv[i], begin, p - begin);
        sp->exec_argv[i][begin - p] = '\0';
        TRACE("argv[%d]: %s\n", i, sp->exec_argv[i]);
#endif
        if (*p == '\n')
            p++;
        if (*p == '\0')
            break;
        i++;
    }
    *ep = p;

#if defined(win_nt)
    cmdline[cmdlen] = '\0';
    TRACE("cmdline: %s\n", cmdline);
#endif
}


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
int sp_start(sp_t *caller_sp,
             sp_pump_t pump_func,
             char *arg_fmt,
             ...)
{
    sp_t                sp;
    char                *s;
    unsigned            i;
    unsigned            j;
    int                 ret;
    char                *p;
    int                 index;
    char                *args;
    char                err_buf[200];
    
    if (TraceFp == NULL &&
        (s = getenv("SUMP_PUMP_TRACE")) != NULL &&
        strlen(s) > 0)
    {
        if (!strcmp(s, "<stdout>"))
            TraceFp = stdout;
        else if (!strcmp(s, "<stderr>"))
            TraceFp = stderr;
        else
        {
            TraceFp = fopen(s, "a+");
            if (TraceFp == NULL)
                fprintf(stderr, "can't open SUMP_PUMP_TRACE=%s\n", s);
            else
            {
                time_t  now = time(NULL);
                trace("begin new sump pump trace, %s", ctime(&now));
            }
        }
    }
    
    *caller_sp = NULL;
    sp = (sp_t)calloc(1, sizeof(struct sump));
    if (sp == NULL)
        return (SP_MEM_ALLOC_ERROR);
    sp->error_buf_size = ERROR_BUF_SIZE;
    sp->error_buf = (char *)calloc(1, sp->error_buf_size);
    if (sp->error_buf == NULL)
        return (SP_MEM_ALLOC_ERROR);
    *caller_sp = sp;
    
    /* fill in default parameters */
    sp->pump_arg = NULL;
    sp->num_threads = get_logical_processor_count(); /* default thread count */
    sp->num_in_bufs = 3 * sp->num_threads;
    sp->num_tasks = 3 * sp->num_threads;
    sp->in_buf_size = (1 << 18);
    sp->num_outputs = 1;
    sp->out = (struct sump_out *)calloc(1, sizeof(struct sump_out));
    sp->out[0].buf_size = (1 << 18);
    sp->delimiter = (void *)"\n";
    sp->rec_size = 0;

    sp->pump_func = pump_func;

    if ((p = getenv("SUMP_PUMP")) == NULL)
    {
        /* allocate minimal string */
        args = (char *)calloc(1, 1);
        args[0] = '\0';
    }
    else
    {
        args = (char *)calloc(strlen(p) + 2, 1);
        memcpy(args, p, strlen(p));
        /* add newline to separate potential following commands */
        args[strlen(p)] = '\n';
        args[strlen(p) + 1] = '\0';
    }

    if (arg_fmt != NULL)
    {
        va_list ap;
        size_t  args_size;

        p = args;
        va_start(ap, arg_fmt);
#if defined(win_nt)
        args_size = _vscprintf(arg_fmt, ap);
#else
        args_size = vsnprintf(NULL, 0, arg_fmt, ap);
#endif
        va_end(ap);
        args = (char *)calloc(strlen(p) + args_size + 1, 1);
        memcpy(args, p, strlen(p));
        va_start(ap, arg_fmt);
        if (vsnprintf(args + strlen(p), args_size + 1, arg_fmt, ap) !=
            args_size)
        {
            start_error(sp, "sp_start: "
                        "vnsprintf failed to return %d\n", args_size);
            return (sp->error_code);
        }
        va_end(ap);
        free(p);   /* free copy of environment string */
        TRACE("sp_start args: '%s'\n", args);
    }

    for (p = args; ; )
    {
        /* ignore leading white space chars */
        while (isspace(*(unsigned char *)p))
            p++;
        if (*p == '\0')
            break;
        if (*p++ != '-')
        {
            /* Must be the name of an external program, and possibly
             * some command line arguments for it.
             */
            p--;
            sp->flags |= SP_EXEC;
#if defined(SUMP_PIPE_STDERR)
            if (sp->num_outputs == 1)
            {
                sp->out = (struct sump_out *)
                    realloc(sp->out, 2 * sizeof(struct sump_out));
                if (sp->out == NULL)
                {
                    start_error(sp, "set_num_outputs, realloc() failed\n");
                    return (sp->error_code);
                }
                memset(sp->out + 1, 0, sizeof(struct sump_out));
                sp->out[1].buf_size = sp->out[0].buf_size;
                sp->num_outputs = 2;
            }
#endif
            get_exec_args(sp, &p);
            if (sp->error_code)
                return (sp->error_code);
        }
        else if (scan("ASCII", &p) || scan("UTF_8", &p))
        {
            sp->flags |= SP_UTF_8;
        }
        else if (scan("DEFAULT_FILE_MODE=", &p))
        {
            if (scan("BUFFERED", &p) || scan("BUF", &p))
                Default_file_mode = MODE_BUFFERED;
            else if (scan("DIRECT", &p) || scan("DIR", &p))
                Default_file_mode = MODE_DIRECT;
            else
                syntax_error(sp, p, "unrecognized file access mode");
        }
        else if (scan("GROUP_BY", &p) || scan("GROUP", &p))
            sp->flags |= SP_GROUP_BY;
        else if (scan("IN_BUFS=", &p))
            sp->num_in_bufs = (unsigned)get_numeric_arg(sp, &p);
        else if (scan("IN_BUF_SIZE=", &p))
        {
            sp->in_buf_size = (ssize_t)get_numeric_arg(sp, &p);
            sp->in_buf_size *= (ssize_t)get_scale(&p);
        }
        else if (scan("IN_FILE=", &p) || scan("IN=", &p))
        {
            /* get input file here */
            sp->in_file = get_string_arg(&p);
            sp->in_file_alloc = TRUE;
        }
        else if (scan("OUT_BUF_SIZE", &p))
        {
            size_t  size;
            double  incr;
                
            if (*p == '=')   /* if no index in square brackets */
            {
                index = 0;   /* default to index 0 */
                p++;
            }
            else
                index = get_output_index(sp, &p);
            if (*p == '.')
                size = 0;
            else
                size = (int)get_numeric_arg(sp, &p);
            if (*p == '.' || *p == 'x' || *p == 'X')
            {
                sp->out[index].buf_size_mult = (double)size;
                if (*p == '.')
                {
                    p++;
                    incr = 0.1;
                    while (*p >= '0' && *p <= '9')
                    {
                        sp->out[index].buf_size_mult +=
                            (*p - '0') * incr;
                        incr *= 0.1;
                        p++;
                    }
                }
                if (*p != 'x' && *p != 'X')
                {
                    start_error(sp, "sp_start: "
                                "out buf size factor must end with 'x'\n");
                    return (sp->error_code);
                }
                p++;
                sp->out[index].size_specified = FALSE;
            }
            else
            {
                sp->out[index].buf_size = size;
                sp->out[index].buf_size *= (size_t)get_scale(&p);
                sp->out[index].size_specified = TRUE;
            }
        }
        else if (scan("OUT_FILE", &p) || scan("OUT", &p))
        {
            if (*p == '=')   /* if no index in square brackets */
            {
                index = 0;   /* default to index 0 */
                p++;
            }
            else
                index = get_output_index(sp, &p);
            if (index >= sp->num_outputs)
            {
                start_error(sp, "sp_start: "
                            "output file index must be less than the "
                            "number of output files\n");
                return (sp->error_code);
            }
            sp->out[index].file = get_string_arg(&p);
            sp->out[index].file_alloc = TRUE;
        }
        else if (scan("OUTPUTS=", &p))
        {
            unsigned        num_outputs;
                
            num_outputs = (unsigned)get_numeric_arg(sp, &p);
            if (num_outputs > sp->num_outputs)
            {
                sp->out = (struct sump_out *)realloc(sp->out,
                                                     num_outputs * sizeof(struct sump_out));
                if (sp->out == NULL)
                {
                    start_error(sp, "set_num_outputs, realloc() failed\n");
                    return (sp->error_code);
                }
                for (i = sp->num_outputs; i < num_outputs; i++)
                {
                    memset(sp->out + i, 0, sizeof(struct sump_out));
                    sp->out[i].buf_size = sp->out[0].buf_size;
                }
            }
            sp->num_outputs = num_outputs;
        }
        else if (scan("WHOLE_BUF", &p) || scan("WHOLE", &p))
        {
            sp->flags &= ~SP_UTF_8;
            sp->flags |= SP_WHOLE_BUF;
        }
        else if (scan("REC_SIZE=", &p))
        {
            sp->rec_size = (int)get_numeric_arg(sp, &p);
            sp->flags &= ~SP_UTF_8;
            sp->flags |= SP_FIXED;
            if (sp->rec_size <= 0)
            {
                start_error(sp, "sp_start: "
                            "REC_SIZE must be greater than 0\n");
                return (sp->error_code);
            }
        }
        else if (scan("RW_TEST_SIZE=", &p))
        {
            Default_rw_test_size = (size_t)get_numeric_arg(sp, &p);
            Default_rw_test_size *= (size_t)get_scale(&p);
        }
        else if (scan("TASKS=", &p))
        {
            sp->num_in_bufs = sp->num_tasks =
                (unsigned)get_numeric_arg(sp, &p);
        }
        else if (scan("THREADS=", &p))
        {
            int     num_threads;
                
            num_threads = (int)get_numeric_arg(sp, &p);
            if (num_threads > 0)
                sp->num_threads = (unsigned)num_threads;
        }
        else
            syntax_error(sp, p, "unrecognized keyword");

        if (sp->error_code)
            return (sp->error_code);
    }
    free(args);

    if (REC_TYPE(sp) == 0)
    {
        start_error(sp, "sp_start: a record type must be specified\n");
        return (sp->error_code);
    }
    if (REC_TYPE(sp) == SP_UTF_8)
    {
        if (strlen((char *)sp->delimiter) > 1)
        {
            start_error(sp, "sp_start: "
                        "currently only single-char delimiters are allowed\n");
            return (sp->error_code);
        }
    }
    else if (REC_TYPE(sp) == SP_UNICODE)
    {
        start_error(sp,
                    "sp_start: unicode records are currently not supported\n");
        return (sp->error_code);
    }
    else if (REC_TYPE(sp) == SP_FIXED)
    {
        /* nothing for now */
    }
    else if (REC_TYPE(sp) == SP_WHOLE_BUF)
    {
        /* nothing for now */
    }
    else
    {
        start_error(sp, "sp_start: multiple record types specified\n");
        return (sp->error_code);
    }

    for (i = 0; i < sp->num_outputs; i++)
    {
        /* if an output buffer absolute size has not been specified
         */
        if (sp->out[i].size_specified == FALSE)
        {
            /* if an output buffer size multiplier was specified, use it */
            if (sp->out[i].buf_size_mult != 0.0)
                sp->out[i].buf_size = (size_t)(sp->in_buf_size *
                                               sp->out[i].buf_size_mult + 0.5);
            else /* default to using 2x the input buffer size */
                sp->out[i].buf_size = 2 * sp->in_buf_size;
        }
        TRACE("out %d: %d\n", i, (int)sp->out[i].buf_size);
    }

    /* alloc rec output buffers for each task */
    sp->task = (sp_task_t)calloc(sp->num_tasks, sizeof(struct sp_task));
    for (i = 0; i < sp->num_tasks; i++)
    {
        sp->task[i].out = (struct task_out *)
            calloc(sp->num_outputs, sizeof (struct task_out));
        sp->task[i].error_buf_size = ERROR_BUF_SIZE;
        sp->task[i].error_buf = (char *)calloc(1, sp->task[i].error_buf_size);
        if (sp->task[i].error_buf == NULL)
            return (SP_MEM_ALLOC_ERROR);
        for (j = 0; j < sp->num_outputs; j++)
        {
            sp->task[i].out[j].buf = (void *)malloc(sp->out[j].buf_size);
            sp->task[i].out[j].size = sp->out[j].buf_size;
        }
        sp->task[i].sp = sp;
    }
    /* alloc input buffers */
    sp->in_buf = (in_buf_t *)calloc(sp->num_in_bufs, sizeof(in_buf_t));
    for (i = 0; i < sp->num_in_bufs; i++)
    {
        size_t  buf_size;

        /* round up buf size to page size multiple */
        buf_size = ((sp->in_buf_size + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
#if defined(win_nt)
        sp->in_buf[i].in_buf = 
            VirtualAlloc(NULL, buf_size, MEM_COMMIT, PAGE_READWRITE);
        if (sp->in_buf[i].in_buf == NULL)
            return (SP_MEM_ALLOC_ERROR);
#else
        init_zero_fd();
        sp->in_buf[i].in_buf = mmap(NULL, buf_size,
                                    PROT_READ | PROT_WRITE,
                                    MAP_PRIVATE, Zero_fd, 0);
        if (sp->in_buf[i].in_buf == MAP_FAILED)
            return (SP_MEM_ALLOC_ERROR);
#endif
        sp->in_buf[i].in_buf_size = sp->in_buf_size;
        sp->in_buf[i].alloc_size = buf_size;
    }

    if (sp->flags & SP_EXEC)
    {
        if (sp->pump_func != NULL)
        {
            start_error(sp, "can't both define a pump function and external program\n");
            return (sp->error_code);
        }
        sp->ex_state = (struct exec_state *)
            calloc(sizeof(struct exec_state), sp->num_threads);
        /* use own internal pump function to pipe to/from external process */
        sp->pump_func = pfunc_exec;

#if !defined(win_nt)
        /* ignore broken pipe signal, failed write()'s return with error */
        signal(SIGPIPE, SIG_IGN);
#endif
    }
    else if (sp->pump_func == NULL)
    {
        start_error(sp, "an external program or pump function needs to be defined\n");
        return (sp->error_code);
    }

    /* create mutexes and conditions */
    pthread_mutex_init(&sp->sump_mtx, NULL);
    pthread_mutex_init(&sp->sp_mtx, NULL);
    pthread_cond_init(&sp->in_buf_readable_cond, NULL);
    pthread_cond_init(&sp->in_buf_done_cond, NULL);
    pthread_cond_init(&sp->task_avail_cond, NULL);
    pthread_cond_init(&sp->task_drained_cond, NULL);
    pthread_cond_init(&sp->task_output_ready_cond, NULL);
    pthread_cond_init(&sp->task_output_empty_cond, NULL);

    /* create thread sump threads */
    sp->thread = (pthread_t *)calloc(sp->num_threads, sizeof(pthread_t));
    for (i = 0; i < sp->num_threads; i++)
    {
        ret =
            pthread_create(&sp->thread[i], NULL, pump_thread_main, (void *)sp);
        if (ret)
            die("pthread_create() failed: %d\n", ret);
        if (sp->flags & SP_EXEC)
        {
            sp->ex_state[i].in.ex = &sp->ex_state[i];
            sp->ex_state[i].out.ex = &sp->ex_state[i];
#if defined(win_nt)
            sp->ex_state[i].in.rd_h = sp->ex_state[i].in.wr_h =
                INVALID_HANDLE_VALUE;
            sp->ex_state[i].out.rd_h = sp->ex_state[i].out.wr_h = 
                INVALID_HANDLE_VALUE;
#else
            sp->ex_state[i].in.rd_fd = sp->ex_state[i].in.wr_fd = INVALID_FD;
            sp->ex_state[i].out.rd_fd = sp->ex_state[i].out.wr_fd = INVALID_FD;
#endif
#if defined(SUMP_PIPE_STDERR)
            sp->ex_state[i].err.ex = &sp->ex_state[i];
            sp->ex_state[i].err.rd_fd = sp->ex_state[i].err.wr_fd = INVALID_FD;
#endif
        }
    }

    for (i = 0; i < sp->num_outputs; i++)
    {
        if (sp->out[i].file != NULL)
        {
            /* start an output file connection */
            sp->out[i].file_sp = sp_open_file_dst(sp, i, sp->out[i].file);
            if (sp->out[i].file_sp == NULL)
            {
                start_error(sp, "%s: %s\n", sp->out[i].file,
                            get_error_msg(0, err_buf, sizeof(err_buf)));
                return (SP_FILE_OPEN_ERROR);
            }
        }
    }
    
    if (sp->in_file != NULL)
    {
        /* start an input file connection */
        sp->in_file_sp = sp_open_file_src(sp, sp->in_file);
        if (sp->in_file_sp == NULL)
        {
            start_error(sp, "%s: %s\n", sp->in_file,
                        get_error_msg(0, err_buf, sizeof(err_buf)));
            return (SP_FILE_OPEN_ERROR);
        }
    } 
    return (SP_OK);
}


/* sp_get_error_string - return an error message string
 *
 * Returns: a string containing a sump pump error message. The string should
 *          NOT be free()'d and is valid until the passed sp_t is sp_free()'d.
 */
const char *sp_get_error_string(sp_t sp, int error_code)
{
    const char  *err_code_str;

    if (sp != NULL && sp->error_buf[0] != '\0')
        return (sp->error_buf);
    if (error_code == 0 && sp != NULL)
        error_code = sp->error_code;
    switch (error_code)
    {
      case SP_FILE_READ_ERROR:
        err_code_str = "SP_FILE_READ_ERROR: file read error";
        break;

      case SP_FILE_WRITE_ERROR:
        err_code_str = "SP_FILE_WRITE_ERROR: file write error";
        break;

      case SP_UPSTREAM_ERROR:
        err_code_str = "SP_UPSTREAM_ERROR: upstream error";
        break;

      case SP_REDUNDANT_EOF:
        err_code_str = "SP_REDUNDANT_EOF: redundant eof";
        break;

      case SP_MEM_ALLOC_ERROR:
        err_code_str = "SP_MEM_ALLOC_ERROR: memory allocation error";
        break;

      case SP_FILE_OPEN_ERROR:
        err_code_str = "SP_FILE_OPEN_ERROR: file open error";
        break;

      case SP_WRITE_ERROR:
        err_code_str = "SP_WRITE_ERROR: write error";
        break;

      case SP_SORT_DEF_ERROR:
        err_code_str = "SP_SORT_DEF_ERROR: sort definition error";
        break;

      case SP_SORT_EXEC_ERROR:
        err_code_str = "SP_SORT_EXEC_ERROR: sort execution error";
        break;

      case SP_GROUP_BY_MISMATCH:
        err_code_str = "SP_GROUP_BY_MISMATCH: group-by mismatch";
        break;

      case SP_SORT_INCOMPATIBLE:
        err_code_str = "SP_SORT_INCOMPATIBLE: a sort sump pump is incompatible"
            " with buffer-at-a-time i/o, specify the input or"
            " output file as a sort parameter instead";
        break;

      case SP_BUF_INDEX_ERROR:
        err_code_str = "SP_BUF_INDEX_ERROR: the specified buffer index is"
            " out-of-range";
        break;

      case SP_OUTPUT_INDEX_ERROR:
        err_code_str = "SP_OUTPUT_INDEX_ERROR: the specified output index is"
            " equal to or larger than the number of outputs";
        break;

      case SP_START_ERROR:
        err_code_str = "SP_START_ERROR: an error occured during sp_start()";
        break;

      case SP_NSORT_LINK_FAILURE:
        err_code_str =
            "SP_NSORT_LINK_FAILURE: link attempt to nsort library failed. "
            "Is nsort installed?";
        break;

      case SP_SORT_NOT_COMPILED:
        err_code_str =
            "SP_SORT_NOT_COMPILED: sump.c has not been compiled to support "
            "sorting";
        break;

      case SP_PUMP_FUNCTION_ERROR:
        err_code_str = "Pump function error";
        break;

      default:
        err_code_str = "Unknown Error";
    }
    return (err_code_str);
}


/* sp_free - free the specified sp_t and its associated state.
 */
void sp_free(sp_t *caller_sp)
{
    sp_t        sp;
    int         i, j;

    sp = *caller_sp;
    if (sp == NULL)
        return;

    sp_wait(sp); /* make sure sump pump has finished */
    
#if !defined(SUMP_PUMP_NO_SORT)
    if (sp->flags & SP_SORT)
    {
        if (sp->sort_temp_buf != NULL)
            free(sp->sort_temp_buf);

        if (sp->nsort_ctx != 0)
            (*Nsort_end)(&sp->nsort_ctx);
    
        if (sp->out != NULL)
            free(sp->out);
    }
    else
#endif
    {
        if (sp->task != NULL)
        {
            for (i = 0; i < sp->num_tasks; i++)
            {
                if (sp->task[i].out != NULL)
                {
                    for (j = 0; j < sp->num_outputs; j++)
                        if (sp->task[i].out[j].buf != NULL)
                            free(sp->task[i].out[j].buf);
                    free(sp->task[i].out);
                }

                if (sp->task[i].error_buf != NULL)
                    free(sp->task[i].error_buf);
            }
            free(sp->task);
        }
        if (sp->in_buf != NULL)
        {
            for (i = 0; i < sp->num_in_bufs; i++)
            {
                if (sp->in_buf[i].in_buf != NULL)
                {
#if defined(win_nt)
                    VirtualFree(sp->in_buf[i].in_buf,
                                sp->in_buf[i].alloc_size, MEM_RELEASE);
#else
                    munmap(sp->in_buf[i].in_buf, sp->in_buf[i].alloc_size);
#endif
                }
            }
            free(sp->in_buf);
        }
        if (sp->ex_state != NULL)
            free(sp->ex_state);

        if (sp->thread != NULL)
        {
            pthread_mutex_destroy(&sp->sump_mtx);
            pthread_mutex_destroy(&sp->sp_mtx);
            pthread_cond_destroy(&sp->in_buf_readable_cond);
            pthread_cond_destroy(&sp->in_buf_done_cond);
            pthread_cond_destroy(&sp->task_avail_cond);
            pthread_cond_destroy(&sp->task_drained_cond);
            pthread_cond_destroy(&sp->task_output_ready_cond);
            pthread_cond_destroy(&sp->task_output_empty_cond);
            free(sp->thread);
        }
    
        if (sp->out != NULL)
        {
            for (i = 0; i < sp->num_outputs; i++)
            {
                if (sp->out[i].file_sp != NULL)
                    sp_file_free(&sp->out[i].file_sp);
                if (sp->out[i].file_alloc && sp->out[i].file != NULL)
                    free(sp->out[i].file);
            }
            free(sp->out);
        }

        if (sp->in_file_sp != NULL)
            sp_file_free(&sp->in_file_sp);
        if (sp->in_file_alloc && sp->in_file != NULL)
            free(sp->in_file);
    }
    if (sp->error_buf != NULL)
        free(sp->error_buf);

    free(sp);
    *caller_sp = NULL;
}


/* sp_file_free - free the specified sp_file_t and its associated state.
 */
void sp_file_free(sp_file_t *caller_sp_file)
{
    sp_file_t sp_file;

    sp_file = *caller_sp_file;
    if (sp_file == NULL)
        return;

    if (sp_file->fname != NULL)
        free(sp_file->fname);
    
    free(sp_file);
    
    *caller_sp_file = NULL;
}
