"""This module defines classes that implements a threaded
producer/consumer system.  A single iterator thread pushes jobs into an
internal queue while a flock of consumer/worker threads do the jobs.  A job
consists of a function and the data applied to the function."""

import time
import threading
import gevent

from gevent.queue import JoinableQueue
from gevent.local import local

from configman import RequiredConfig, Namespace
from configman.converters import class_converter

from socorro.lib.task_manager import (
    default_task_func,
    default_iterator,
    TaskManager
)


#==============================================================================
class GreenletTaskManager(TaskManager):
    """Given an iterator over a sequence of job parameters and a function,
    this class will execute the function in a set of threads."""
    required_config = Namespace()
    # how does one choose how many threads to use?  Keep the number low if your
    # application is compute bound.  You can raise it if your app is i/o
    # bound.  The best thing to do is to test the through put of your app with
    # several values.  For Socorro, we've found that setting this value to the
    # number of processor cores in the system gives the best throughput.
    required_config.add_option(
      'number_of_threads',
      default=4,
      doc='the number of greenlets'
    )

    #--------------------------------------------------------------------------
    def __init__(self, config,
                 job_source_iterator=default_iterator,
                 task_func=default_task_func):
        """the constructor accepts the function that will serve as the data
        source iterator and the function that the threads will execute on
        consuming the data.

        parameters:
            job_source_iterator - an iterator to serve as the source of data.
                                  it can be of the form of a generator or
                                  iterator; a function that returns an
                                  iterator; a instance of an iterable object;
                                  or a class that when instantiated with a
                                  config object can be iterated.  The iterator
                                  must yield a tuple consisting of a
                                  function's tuple of args and, optionally, a
                                  mapping of kwargs.
                                  Ex:  (('a', 17), {'x': 23})
            task_func - a function that will accept the args and kwargs yielded
                        by the job_source_iterator"""
        super(GreenletTaskManager, self).__init__(
            config,
            job_source_iterator,
            task_func
        )
        self.greenlet_list = []  # the greenlet object storage
        self.number_of_greenlets = config.number_of_threads
        self.task_queue = JoinableQueue()

        self.greenlet_local = local()
        self.greenlet_local.name = threading.currentThread().getName()

    #--------------------------------------------------------------------------
    def start(self):
        self.logger.debug('start')
        # start each of the task threads.
        for x in range(self.number_of_greenlets):
            gevent.spawn(
                self.task,
                "%s:greenlet-%02d" % (self.greenlet_local.name, x)
            )

    #--------------------------------------------------------------------------
    #def wait_for_completion(self, waiting_func=None):
        #"""This is a blocking function call that will wait for the queuing
        #thread to complete.

        #parameters:
            #waiting_func - this function will be called every one second while
                           #waiting for the queuing thread to quit.  This allows
                           #for logging timers, status indicators, etc."""
        #self.logger.debug("waiting to join queuingThread")
        #self._responsive_join(self.queuing_thread, waiting_func)

    #--------------------------------------------------------------------------
    def stop(self):
        """This function will tell all threads to quit.  All threads
        periodically look at the value of quit.  If they detect quit is True,
        then they commit ritual suicide.  After setting the quit flag, this
        function will wait for the queuing thread to quit."""
        self.quit = True
        while True:
            size = self.task_queue.qsize()
            if size == 0:
                break
            gevent.sleep(0)

    #--------------------------------------------------------------------------
    def blocking_start(self, waiting_func=None):
        """this function is just a wrapper around the start and
        wait_for_completion methods.  It starts the queuing thread and then
        waits for it to complete.  If run by the main thread, it will detect
        the KeyboardInterrupt exception (which is what SIGTERM and SIGHUP
        have been translated to) and will order the threads to die."""
        try:
            self.start()
            self._do_the_queuing()
            #self.task_queue.join()
            # it only ends if someone hits  ^C or sends SIGHUP or SIGTERM -
            # any of which will get translated into a KeyboardInterrupt
        except KeyboardInterrupt:
            while True:
                try:
                    self.stop()
                    break
                except KeyboardInterrupt:
                    self.logger.warning('We heard you the first time.  There '
                                   'is no need for further keyboard or signal '
                                   'interrupts.  We are waiting for the '
                                   'worker greenlets to stop.  If this app '
                                   'does not halt soon, you may have to send '
                                   'SIGKILL (kill -9)')

    #--------------------------------------------------------------------------
    def quit_check(self):
        """this is the polling function that the threads periodically look at.
        If they detect that the quit flag is True, then a KeyboardInterrupt
        is raised which will result in the threads dying peacefully"""
        gevent.sleep(0)
        if self.quit:
            raise KeyboardInterrupt

    #--------------------------------------------------------------------------
    def _responsive_sleep(self, seconds, wait_log_interval=0, wait_reason=''):
        """When there is litte work to do, the queuing thread sleeps a lot.
        It can't sleep for too long without checking for the quit flag and/or
        logging about why it is sleeping.

        parameters:
            seconds - the number of seconds to sleep
            wait_log_interval - while sleeping, it is helpful if the thread
                                periodically announces itself so that we
                                know that it is still alive.  This number is
                                the time in seconds between log entries.
            wait_reason - the is for the explaination of why the thread is
                          sleeping.  This is likely to be a message like:
                          'there is no work to do'.

        This was also partially motivated by old versions' of Python inability
        to KeyboardInterrupt out of a long sleep()."""

        for x in xrange(int(seconds)):
            self.quit_check()
            if wait_log_interval and not x % wait_log_interval:
                self.logger.info('%s: %dsec of %dsec',
                                 wait_reason,
                                 x,
                                 seconds)
                self.quit_check()
            gevent.sleep(1.0)

    #--------------------------------------------------------------------------
    #def wait_for_empty_queue(self, wait_log_interval=0, wait_reason=''):
        #"""Sit around and wait for the queue to become empty

        #parameters:
            #wait_log_interval - while sleeping, it is helpful if the thread
                                #periodically announces itself so that we
                                #know that it is still alive.  This number is
                                #the time in seconds between log entries.
            #wait_reason - the is for the explaination of why the thread is
                          #sleeping.  This is likely to be a message like:
                          #'there is no work to do'."""
        #seconds = 0
        #while True:
            #if self.task_queue.empty():
                #break
            #self.quit_check()
            #if wait_log_interval and not seconds % wait_log_interval:
                #self.logger.info('%s: %dsec so far',
                                 #wait_reason,
                                 #seconds)
                #self.quit_check()
            #seconds += 1
            #time.sleep(1.0)

    #--------------------------------------------------------------------------
    #def _responsive_join(self, thread, waiting_func=None):
        #"""similar to the responsive sleep, a join function blocks a thread
        #until some other thread dies.  If that takes a long time, we'd like to
        #have some indicaition as to what the waiting thread is doing.  This
        #method will wait for another thread while calling the waiting_func
        #once every second.

        #parameters:
            #thread - an instance of the TaskThread class representing the
                     #thread to wait for
            #waiting_func - a function to call every second while waiting for
                           #the thread to die"""
        #while True:
            #try:
                #thread.join(1.0)
                #if not thread.isAlive():
                    #break
                #if waiting_func:
                    #waiting_func()
            #except KeyboardInterrupt:
                #self.logger.debug('quit detected by _responsive_join')
                #self.quit = True

    #--------------------------------------------------------------------------
    def _kill_worker_threads(self):
        """This function coerces the consumer/worker threads to kill
        themselves.  When called by the queuing thread, one death token will
        be placed on the queue for each thread.  Each worker thread is always
        looking for the death token.  When it encounters it, it immediately
        runs to completion without drawing anything more off the queue."""
        for x in range(self.number_of_greenlets):
            self.task_queue.put((None, None))
        gevent.sleep(0)

    #--------------------------------------------------------------------------
    def _do_the_queuing(self):
        """This is the function responsible for reading the iterator and
        putting contents into the queue.  It loops as long as there are items
        in the iterator.  Should something go wrong with this thread, or it
        detects the quit flag, it will calmly kill its workers and then
        quit itself."""
        self.logger.debug('_do_the_queuing start')
        try:
            for job_params in self._get_iterator():  # may never raise
                                                     # StopIteration
                if job_params is None:
                    self.logger.info("there is nothing to do.  Sleeping "
                                     "for %d seconds" %
                                     self.config.idle_delay)
                    self._responsive_sleep(self.config.idle_delay)
                    continue
                self.quit_check()
                self.logger.debug("queuing job %s", job_params)
                self.task_queue.put((self.task_func, job_params))
            else:
                self.logger.debug("the loop didn't actually loop")
        except Exception:
            self.logger.error('queuing jobs has failed', exc_info=True)
        except KeyboardInterrupt:
            self.logger.debug('quit request detected')
        finally:
            self.quit = True
            self.logger.debug("we're quitting queuing")
            self._kill_worker_threads()
            self.logger.debug("all worker greenlets poisoned")

    #--------------------------------------------------------------------------
    def task(self, worker_name):
        """The main routine for a thread's work.

        The thread pulls tasks from the task queue and executes them until it
        encounters a death token.  The death token is a tuple of two Nones.
        """
        self.greenlet_local.name = worker_name
        try:
            self.config.logger.debug('%s starting', worker_name)
            while True:
                function, arguments = self.task_queue.get()
                if function is None:
                    self.config.logger.debug(
                        '%s consumes poison',
                        worker_name
                    )
                    break
                if self.quit:
                    continue
                try:
                    try:
                        args, kwargs = arguments
                    except ValueError:
                        args = arguments
                        kwargs = {}
                    function(*args, **kwargs)  # execute the task
                except Exception:
                    self.config.logger.error("Error in processing a job",
                                             exc_info=True)
                except KeyboardInterrupt:
                    self.quit = True
                    self.config.logger.info(
                        '%s quit request detected',
                        worker_name
                    )
        except Exception:
            self.config.logger.critical("Failure in task_queue", exc_info=True)
        self.config.logger.debug("%s dies", worker_name)

    #--------------------------------------------------------------------------
    def executor_identity(self):
        """this function is likely to be called via the configuration parameter
        'executor_identity' at the root of the self.config attribute of the
        application.  It is most frequently used in the Pooled
        ConnectionContext classes to ensure that connections aren't shared
        between threads, greenlets, or whatever the unit of execution is.
        This is useful for maintaining transactional integrity on a resource
        connection."""
        try:
            return self.greenlet_local.name
        except AttributeError:
            # if identity has not been set, then it is not a greenlet of
            # unknown origin and not created by this class.  Since execution
            # has gotten here, this greenlet is about to be used for something
            # deep in our system, so we'd better jolly well give it an identity
            if not hasattr(self, 'unknown_greenlet_counter'):
                from itertools import count
                self.unknown_greenlet_counter = count()
            self.greenlet_local.name = (
                '%s:UnknownGreenlet-%03d' % (
                    threading.currentThread().getName(),
                    self.unknown_greenlet_counter.next()
                )
            )
            return

