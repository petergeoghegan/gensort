/* sump_win.c - Windows-specific code to be directly included in sump.c 
 *              for the SUMP Pump(TM) MP/CMP parallel data pump library.
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
#include <sys/timeb.h>
#include <io.h>
#include <process.h>
#include <time.h>
#include <sys/timeb.h>

/* Convert the two DWORDs in a FILETIME (second and 100ns units) 
 * into a single uint64_t in microseconds
 */
#define FILETIME_TO_US(filetime) \
    (((uint64_t) filetime.dwLowDateTime + ((uint64_t) filetime.dwHighDateTime << 32)) / 10)


/* pthread_join - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_join(pthread_t th, void **value_ptr)
{
    int         ret;

    if (th.handle_closed == FALSE)
    {
        /* wait for thread to exit */
        if ((ret = WaitForSingleObject(th.h, INFINITE)) != WAIT_OBJECT_0) 
            TRACE("wait for thread 0x%x failed: %d\n", th, ret);
        ret = GetExitCodeThread(th.h, &th.exit_code);
        TRACE("pthread_join: tread exited\n");
        if (ret && th.exit_code == STILL_ACTIVE)
            TRACE("pthread_join: thread %d has not exited yet", th);
        CloseHandle(th.h);
        th.handle_closed = TRUE;
    }
    else
        ret = 1;
    if (value_ptr != NULL)
        *value_ptr = (void *)th.exit_code;
    return (ret ? 0 : ESRCH);
}


/* pthread_create - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_create(pthread_t *t, void *dummy, void *(*main)(void *), void *arg)
{
    t->handle_closed = FALSE;
    t->exit_code = 0;
    t->h = (HANDLE) _beginthreadex(NULL, 0, (LPTHREAD_START_ROUTINE)main, 
                                   arg, 0, &t->id);
    TRACE("pthread_create returns HANDLE: %d\n", t->h);
    if (t->h == NULL)
        return (GetLastError());
    return (0);
}

/* pthread_mutex_init - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_mutex_init(pthread_mutex_t *mutex, pthread_mutexattr_t *attr)
{
    HANDLE      h;

    h = CreateMutex(NULL, FALSE, NULL);
    if (h == NULL)
        die("pthread_mutex_init: can't create mutex");
    *mutex = h;
    return (0);
}

/* pthread_mutex_destroy - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_mutex_destroy(pthread_mutex_t *mutex)
{
    int ret;

    ret = CloseHandle(*mutex);
    return (!ret);
}

/* pthread_mutex_lock - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_mutex_lock(pthread_mutex_t *mutex)
{
    int ret;
    pthread_mutex_t     was = *mutex;

    if (*mutex == INVALID_HANDLE_VALUE)
        die("pthread_mutex_lock: invalid mutex %p", mutex);
    if ((ret = WaitForSingleObject(*mutex, INFINITE)) != WAIT_OBJECT_0)
        die("pthread_mutex_lock: didn't get mutex: %d[%d]@%p %d %d", *mutex, was, mutex, ret, GetLastError());
    return (0);
}

/* pthread_mutex_unlock - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_mutex_unlock(pthread_mutex_t *mutex)
{
    if (*mutex == INVALID_HANDLE_VALUE)
        die("pthread_mutex_unlock: invalid mutex %p %d", mutex, *mutex);
    return (!ReleaseMutex(*mutex));
}

/* pthread_mutexattr_* routines (stubs)
 */
int pthread_mutexattr_init(pthread_mutexattr_t *h) { return (0); }
int pthread_mutexattr_settype(pthread_mutexattr_t *h, int type) { return (0); }
int pthread_mutexattr_destroy(pthread_mutexattr_t *h) { return (0); }

/* pthread_cond_init - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_cond_init(pthread_cond_t *cond, pthread_condattr_t *attr)
{
    HANDLE      h;

    h = CreateEvent(NULL, TRUE, FALSE, NULL);
    if (h == NULL)
        die("pthread_cond_init: can't create event");
    *cond = h;
    return (0);
}

/* pthread_cond_destroy - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_cond_destroy(pthread_cond_t *cond)
{
    int ret;

    ret = CloseHandle(*cond);
    TRACE("pthread_cond_destroy(&%p [%d]) closed %s\n", cond, *cond, strerror(GetLastError()));
    return (!ret);
}

/* pthread_cond_signal - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_cond_signal(pthread_cond_t *cond)
{
#if defined(DEBUG1)     /* let SetEvent complain */
    if (*cond == INVALID_HANDLE_VALUE)
        die("pthread_cond_signal: invalid cond %p", cond);
#endif
    if (SetEvent(*cond) == 0)
        die("pthread_cond_signal(%p: %d): %s", cond, *cond, strerror(GetLastError()));
    TRACE("pthread_cond_signal(&%p [%d])\n", cond, *cond);
    return (0);
}

/* cond_wait - common code to Ordinal's implementation of
 *              pthread_cond_wait() and pthread_cond_timedwait()
 */
static int cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex, int timeout)
{
    int ret;
    
    /* current thread already holds mutex */
    /* clear event */
    if (ResetEvent(*cond) == 0)
        die("cond_wait: can't reset event %p %d: %s", cond, *cond, strerror(GetLastError()));
    
    /* release mutex before waiting for cond to be signalled */
    if (ReleaseMutex(*mutex) == 0)
        die("cond_wait: %p can't release mutex %p %d : %s", cond, mutex, *mutex, strerror(GetLastError()));

    /* Wait for event/cond signal
     *
     * There is a race condition here since we have just released the
     * mutex, another thread could now signal the condition (event).
     * That's OK since we are using manual-reset events, i.e. the event
     * should still be signalled we when call WaitForSingleObject()
     * below.  Unless of course another task examines the condition and
     * decides to wait for it, and the above ResetEvent() is called to
     * reset the event.  But that should also be OK because if another
     * task decides to wait on the condition, it should be OK for this
     * task to wait also.
     */
    switch (WaitForSingleObject(*cond, timeout))
    {
      case WAIT_OBJECT_0:
        ret = 0;
        break;

      case WAIT_TIMEOUT:
        ret = ETIMEDOUT;
        break;

      default:
        die("cond_wait: can't wait for cond %p", cond);
    }
    /* get mutex before returning */
    switch (WaitForSingleObject(*mutex, INFINITE))
    {
      case WAIT_OBJECT_0:       /* got mutex */
        break;

      case WAIT_ABANDONED:
        die("cond_wait: cond %p mutex %p %d abandoned on wait", cond, mutex, *mutex);

      case WAIT_TIMEOUT:        /* in theory this can't happen with an
                                   INFINITE wait, but NT can be strange */
        die("cond_wait: cond %p mutex %p %d timeout on wait", cond, mutex, *mutex);

      default:                  /* should be WAIT_FAILED */
        die("cond_wait: cond %p mutex %p %d wait failed: %s", cond, mutex, *mutex, strerror(GetLastError()));
    }
    return (ret);
}

/* pthread_cond_wait - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
    /* this implementation always uses timed waits of 1 millisecond to
     * avoid a race condition.
     */
    return (cond_wait(cond, mutex, 1));
}

/* pthread_cond_timedwait - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_cond_timedwait(pthread_cond_t *cond, pthread_mutex_t *mutex, struct timespec *ts)
{
    /* just wait one tick for now */
    return (cond_wait(cond, mutex, 1));
}

/* pthread_exit - SUMP Pump on NT implementation of POSIX thread routine.
 */
void pthread_exit(void *status)
{
    _endthreadex((UINT)(size_t)status);
}

/* pthread_detatch - SUMP Pump on NT implementation of POSIX thread routine.
 */
int pthread_detach(pthread_t th) { return (0); }



/* gettimeofday - Windows implementation of Unix gettimeofday()
 */
int gettimeofday(struct timeval *tv, void *not_implemented)
{
    static uint64_t     frequency = 0;
    LARGE_INTEGER       largeint;

    if (frequency == 0)
    {
    	if (QueryPerformanceFrequency(&largeint) == 0)
	    die("gettimeofday: QueryPerformanceFrequency error\n");
	frequency = largeint.QuadPart;
    }
    if (QueryPerformanceCounter(&largeint) == 0)
	die("gettimeofday: QueryPerformanceCounter error\n");

    tv->tv_sec = largeint.QuadPart / frequency;
    tv->tv_usec = (1000000 * (largeint.QuadPart % frequency)) / frequency;

    return (0);
}


#define RETRY_LIMIT     10

int aio_read(struct aiocb *aio)
{
    BOOL        ret;
    int         err;
    int         retry;
    
    aio->sump_eof = FALSE;
    aio->sump_errno = 0;
    for (retry = 0; retry < RETRY_LIMIT; retry++)
    {
        ResetEvent(aio->sump_over.hEvent);
        aio->sump_over.Offset = (DWORD)aio->aio_offset;
        aio->sump_over.OffsetHigh = (DWORD)(aio->aio_offset >> 32);
        ret = ReadFile(aio->aio_fildes,
                       aio->aio_buf,
                       aio->aio_nbytes,
                       NULL,
                       &aio->sump_over);
        err = GetLastError();
        if (ret)
            return (0);      /* success */
        if (ret == 0)
        {
            if (err == ERROR_HANDLE_EOF)
            {
                aio->sump_eof = TRUE;
                SetEvent(aio->sump_over.hEvent);
                return (0);
            }
            if (err == ERROR_IO_PENDING)
                return (0);     /* success so far */
            if (err == ERROR_WORKING_SET_QUOTA ||
                err == ERROR_INVALID_USER_BUFFER || 
                err == ERROR_NOT_ENOUGH_MEMORY)
            {
                continue;
            }
        }
    }
    /* error occured, retries have been exhausted for transitory errors */
    aio->sump_errno = err;
    SetEvent(aio->sump_over.hEvent);
    return (-1);
}


int aio_write(struct aiocb *aio)
{
    BOOL        ret;
    int         err;
    int         retry;
    
    aio->sump_eof = FALSE;
    aio->sump_errno = 0;
    for (retry = 0; retry < RETRY_LIMIT; retry++)
    {
        ResetEvent(aio->sump_over.hEvent);
        aio->sump_over.Offset = (DWORD)aio->aio_offset;
        aio->sump_over.OffsetHigh = (DWORD)(aio->aio_offset >> 32);
        ret = WriteFile(aio->aio_fildes,
                        aio->aio_buf,
                        aio->aio_nbytes,
                        NULL,
                        &aio->sump_over);
        err = GetLastError();
        if (ret)
            return (0);      /* success */
        if (ret == 0)
        {
            if (err == ERROR_IO_PENDING)
                return (0);     /* success so far */
            if (err == ERROR_WORKING_SET_QUOTA ||
                err == ERROR_INVALID_USER_BUFFER || 
                err == ERROR_NOT_ENOUGH_MEMORY)
            {
                continue;
            }
        }
    }
    /* error occured, retries have been exhausted for transitory errors */
    aio->sump_errno = err;
    SetEvent(aio->sump_over.hEvent);
    return (-1);
}


int aio_suspend(const struct aiocb * const cblist[], int n, void *timeout)
{
    int ret;

    if (n != 1)
        die("aio_suspend: n arg is not 1: %d\n", n);
    if (timeout != NULL)
        die("aio_suspend: timeout is not NULL\n");
    if (cblist[0]->sump_eof)
        return (0);
    ret = WaitForSingleObject(cblist[0]->sump_over.hEvent, INFINITE);
    if (ret != WAIT_OBJECT_0) 
        die("aio_suspend: WaitForSingleObject failed: %d\n", ret);
    return (0);
}


ssize_t aio_return(struct aiocb *aio)
{
    DWORD       result = 0;
    
    if (aio->sump_eof)
        return (0);
    if (!GetOverlappedResult(aio->aio_fildes, &aio->sump_over, &result, TRUE))
        aio->sump_errno = GetLastError();
    return (aio->sump_errno ? -1 : result);
}


int aio_error(struct aiocb *aio)
{
    return (aio->sump_errno);
}

