import time
import threading
import Queue

import socorro.lib.util as sutil
from  socorro.lib.util import reportExceptionAndContinue
import socorro.lib.threadlib as thr

from configman import RequiredConfig, Namespace
from configman.converters import class_converter

#------------------------------------------------------------------------------
OK = 1
FAILURE = 0
RETRY = 2


#------------------------------------------------------------------------------
def default_task_func(a_param):
    pass


#------------------------------------------------------------------------------
def default_iterator():
    for x in range(10):
        yield ((x,), {})
    while True:
        yield None


#------------------------------------------------------------------------------
def respond_to_SIGTERM(signalNumber, frame):
    """ these classes are instrumented to respond to a KeyboardInterrupt by
        cleanly shutting down.  This function, when given as a handler to for
        a SIGTERM event, will make the program respond to a SIGTERM as neatly
        as it responds to ^C.
    """
    raise KeyboardInterrupt


#==============================================================================
class ThreadedTaskManager(RequiredConfig):
    """Given an iterator over a sequence of job parameters and a function,
    this class will execute the the function in a set of threads."""
    required_config = Namespace()
    required_config.add_option('idle_delay',
                               default=7,
                               doc='the delay in seconds if no job is found')
    required_config.add_option('job_source_iterator',
                               default=default_iterator,
                               doc='an iterator or callable that will '
                               'return an iterator',
                               from_string_converter=class_converter)
    required_config.add_option('task_func',
                               default=default_task_func,
                               doc='a callable that accomplishes a task',
                               from_string_converter=class_converter)
    required_config.add_option('number_of_threads',
                               default=10,
                               doc='the number of threads')
    required_config.add_option('maximum_queue_size',
                               default=10,
                               doc='the maximum size of the internal queue')

    #--------------------------------------------------------------------------
    def __init__(self, config, job_source_iterator=default_iterator,
                  task_func=default_task_func):
        super(ThreadedTaskManager, self).__init__()
        self.config = config
        self.logger = config.logger
        self.job_param_source_iter = config.setdefault('job_source_iterator',
                                                   job_source_iterator)
        self.task_func = config.setdefault('task_func', task_func)

        self.thread_list = []
        self.number_of_threads = config.number_of_threads
        self.task_queue = Queue.Queue(config.maximum_queue_size)
        for x in range(self.number_of_threads):
            new_thread = TaskThread(config, self)
            self.thread_list.append(new_thread)
            new_thread.start()

        self.quit = False
        self.logger.debug('finished init')

    #--------------------------------------------------------------------------
    def start(self):
        self.logger.debug('start')
        self.queuing_thread = threading.Thread(name="QueuingThread",
                                              target=self._queuing_thread_func)
        self.queuing_thread.start()

    #--------------------------------------------------------------------------
    def wait_for_completion(self, waitingFunc=None):
        self.logger.debug("waiting to join queuingThread")
        self._responsive_join(self.queuing_thread, waitingFunc)

    #--------------------------------------------------------------------------
    def stop(self):
        self.quit = True
        self.wait_for_completion()

    #--------------------------------------------------------------------------
    def blocking_start(self):
        try:
            self.start()
            self.wait_for_completion()  # though, it only ends if someone hits
                                      # ^C or sends SIGHUP or SIGTERM - any
                                      # of which will get translated into a
                                      # KeyboardInterrupt exception
        except KeyboardInterrupt:
            while True:
                try:
                    self.stop()
                    break
                except KeyboardInterrupt:
                    self.logger.warning('We heard you the first time.  There '
                                   'is no need for further keyboard or signal '
                                   'interrupts.  We are waiting for the '
                                   'worker threads to stop.  If this app '
                                   'does not halt soon, you may have to send '
                                   'SIGKILL (kill -9)')

    #--------------------------------------------------------------------------
    def _quit_check(self):
        if self.quit:
            raise KeyboardInterrupt

    #--------------------------------------------------------------------------
    def _responsive_sleep(self, seconds, waitLogInterval=0, waitReason=''):
        for x in xrange(int(seconds)):
            self._quit_check()
            if waitLogInterval and not x % waitLogInterval:
                self.logger.info('%s: %dsec of %dsec',
                                 waitReason,
                                 x,
                                 seconds)
                self._quit_check()
            time.sleep(1.0)

    #--------------------------------------------------------------------------
    def _responsive_join(self, thread, waiting_func=None):
        while True:
            try:
                thread.join(1.0)
                if not thread.isAlive():
                    #self.logger.debug('%s is dead', str(thread))
                    break
                if waiting_func:
                    waiting_func()
            except KeyboardInterrupt:
                self.logger.debug('quit detected by responsiveJoin')
                self.quit = True

    #--------------------------------------------------------------------------
    def _get_iterator(self):
        try:
            return self.job_param_source_iter(self.config)
        except TypeError:
            try:
                return self.job_param_source_iter()
            except TypeError:
                return self.job_param_source_iter

    #--------------------------------------------------------------------------
    def _kill_worker_threads(self):
        for x in range(self.number_of_threads):
            self.task_queue.put((None, None))
        self.logger.debug("waiting for standard worker threads to stop")
        for t in self.thread_list:
            t.join()

    #--------------------------------------------------------------------------
    def _queuing_thread_func(self):
        self.logger.debug('_queuing_thread_func start')
        try:
            for job_params in self._get_iterator():  # may never raise
                                                     # StopIteration
                if job_params is None:
                    self.logger.info("there is nothing to do.  Sleeping "
                                     "for %d seconds" %
                                     self.config.idle_delay)
                    self._responsive_sleep(self.config.idle_delay)
                    continue
                self._quit_check()
                self.logger.debug("queuing standard job %s",
                                  job_params)
                self.task_queue.put((self.task_func, job_params))
        except Exception:
            self.logger.warning('queuing jobs has failed')
            sutil.reportExceptionAndContinue(self.logger)
        except KeyboardInterrupt:
            self.logger.debug('queuingThread gets quit request')
        finally:
            self.quit = True
            self.logger.debug("we're quitting queuingThread")
            self._kill_worker_threads()
            self.logger.debug("all worker threads stopped")


#==============================================================================
class IteratorWorkerFrameworkWithRetry(ThreadedTaskManager):
    # pragma: no cover
    """likely deprecated"""

    #--------------------------------------------------------------------------
    def __init__(self, config,
                 name='mill',
                 job_source_iterator=default_iterator,
                 task_func=default_task_func):
        super(IteratorWorkerFrameworkWithRetry, self).__init__(config,
                                                        job_source_iterator,
                                                        task_func)
        self.inner_task_func = self.task_func
        self.task_func = self.retryTaskFuncWrapper

    #--------------------------------------------------------------------------
    @staticmethod
    def backoffSecondsGenerator():
        seconds = [10, 30, 60, 120, 300]
        for x in seconds:
            yield x
        while True:
            yield seconds[-1]

#------------------------------------------------------------------------------
    def retryTaskFuncWrapper(self, *args):
        backoffGenerator = self.backoffSecondsGenerator()
        try:
            while True:
                result = self.inner_task_func(*args)
                if self.quit:
                    break
                if result in (OK, FAILURE):
                    return
                waitInSeconds = backoffGenerator.next()
                self.logger.critical('failure in task - retry in %s seconds',
                                     waitInSeconds)
                self._responsive_sleep(waitInSeconds,
                                       10,
                                       "waiting for retry after failure in "
                                           "task")
        except KeyboardInterrupt:
            return


#==============================================================================
class TaskThread(threading.Thread):
    """This class represents a worker thread for the TaskManager class"""

    #--------------------------------------------------------------------------
    def __init__(self, config, manager):
        """Initialize a new thread.
        """
        super(TaskThread, self).__init__()
        self.manager = manager
        self.config = config

    #--------------------------------------------------------------------------
    def run(self):
        """The main routine for a thread's work.

        The thread pulls tasks from the manager's task queue and executes
        them until it encounters a task with a function that is None.
        """
        try:
            while True:
                aFunction, arguments = self.manager.task_queue.get()
                if aFunction is None:
                    break
                try:
                    try:
                        args, kwargs = arguments
                    except ValueError:
                        args = arguments
                        kwargs = {}
                    aFunction(*args, **kwargs)
                except Exception, x:
                    reportExceptionAndContinue(logger=self.config.logger)
        except KeyboardInterrupt:
            self.logger.info('%s caught KeyboardInterrupt')
            thread.interrupt_main()  # only needed if signal handler not
                                     # registerd
        except Exception, x:
            self.logger.critical("Failure in task_queue")
            import logging
            reportExceptionAndContinue(logger=self.config.logger,
                                       loggingLevel=logging.CRITICAL)
