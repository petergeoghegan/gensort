/* sump_win.h - Header file for Windows-specific code in sump.c and sump_win.c
 *              for the SUMP Pump(TM) SMP/CMP parallel data pump library.
 *
 * $Revision$
 *
 * Copyright (C) 2011, Ordinal Technology Corp, http://www.ordinal.com
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
#include <windows.h>

#ifdef  _WIN64
typedef __int64    ssize_t;
#else
typedef __w64 int  ssize_t;
#endif  // !_WIN64

typedef __int64		i8;
typedef unsigned __int64 u8;

typedef struct
{
    DWORD       id;
    HANDLE      h;
    long        exit_code;
    int         handle_closed;
}
pthread_t;

typedef HANDLE pthread_mutex_t;
typedef HANDLE pthread_cond_t;
#if defined(WINPTHREAD)
typedef HANDLE pthread_key_t;
#else
typedef DWORD pthread_key_t;
#endif
typedef HANDLE pthread_mutexattr_t;
typedef HANDLE pthread_condattr_t;
typedef int pid_t;

#define strncasecmp _strnicmp
#define ESRCH 3
#define ETIMEDOUT 145

# define PTHREAD_MUTEX_INITIALIZER	INVALID_HANDLE_VALUE
# define PTHREAD_KEY_INITIALIZER	TLS_OUT_OF_INDEXES
# define PTHREAD_CANCELED		((void *) 1L)

/* pthread_create() is not defined, use _beginthreadex() directly */
int pthread_join(pthread_t, void **);
void pthread_exit(void *status);
int pthread_detach(pthread_t);
# define pthread_self()		GetCurrentThreadId()
# define pthread_equal(pt1, pt2)	((pt1) == (pt2))

int pthread_create(pthread_t *t, void *dummy, void *(*start)(void *), void *arg);
int pthread_mutex_init(pthread_mutex_t *, pthread_mutexattr_t *);
int pthread_mutex_destroy(pthread_mutex_t *);
int pthread_mutex_lock(pthread_mutex_t *);
int pthread_mutex_trylock(pthread_mutex_t *);
int pthread_mutex_unlock(pthread_mutex_t *);
int pthread_mutexattr_init(pthread_mutexattr_t *h);
int pthread_mutexattr_settype(pthread_mutexattr_t *h, int type);
int pthread_mutexattr_destroy(pthread_mutexattr_t *h);

int pthread_cond_init(pthread_cond_t *, pthread_condattr_t *);
int pthread_cond_destroy(pthread_cond_t *);
int pthread_cond_signal(pthread_cond_t *);
# define pthread_cond_broadcast	pthread_cond_signal
int pthread_cond_wait(pthread_cond_t *, pthread_mutex_t *);
int pthread_cond_timedwait(pthread_cond_t *, pthread_mutex_t *,
			   struct timespec *);

int pthread_key_create(pthread_key_t *, void (*)(void *));
int pthread_setspecific(pthread_key_t, const void *);
void *pthread_getspecific(pthread_key_t);
int pthread_key_delete(pthread_key_t);

char *nt_strerror(int error, char *buf, size_t buf_size);

int gettimeofday(struct timeval *, void *);


# define O_DIRECT	0x8000

struct aiocb {
    HANDLE      aio_fildes;	/* file HANDLE */
    void        *aio_buf; 	/* Data buffer */
    size_t      aio_nbytes;	/* number of bytes of data */
    HANDLE      aio_helper_event;/* helper waits on this before starting io */
    int64_t     aio_offset;	/* file offset position */
    OVERLAPPED  sump_over;
    int         sump_errno;
    int         sump_eof;
};

int aio_read(struct aiocb *aiocbp);
int aio_write(struct aiocb *aiocbp);
int aio_suspend(const struct aiocb * const cblist[], int n, void *timeout);
ssize_t aio_return(struct aiocb *aiocbp);
int aio_error(struct aiocb *aiocbp);


