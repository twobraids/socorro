import time
import threading

import socorro.lib.util as sutil
import socorro.lib.threadlib as thr

from configman import RequiredConfig, Namespace
from configman.converters import class_converter

#------------------------------------------------------------------------------
OK = 1
FAILURE = 0
RETRY = 2

#------------------------------------------------------------------------------
def default_task_func(jobTuple):
    pass


#------------------------------------------------------------------------------
def default_iterator():
    for x in range(10):
        yield x
    while True:
        yield None


#------------------------------------------------------------------------------
def respondToSIGTERM(signalNumber, frame):
    """ these classes are instrumented to respond to a KeyboardInterrupt by
        cleanly shutting down.  This function, when given as a handler to for
        a SIGTERM event, will make the program respond to a SIGTERM as neatly
        as it responds to ^C.
    """
    raise KeyboardInterrupt


#==============================================================================
class IteratorWorkerFramework(RequiredConfig):
    """ """
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

    #--------------------------------------------------------------------------
    def __init__ (self, config, job_source_iterator=default_iterator,
                  task_func=default_task_func):
        super(IteratorWorkerFramework, self).__init__()
        self.config = config
        self.logger = config.logger
        self.jobSourceIterator = config.setdefault('job_source_iterator',
                                                   job_source_iterator)
        self.task_func = config.setdefault('task_func', task_func)

        # setup the task manager to a queue size twice the size of the number
        # of threads in use.  Because some mechanisms that feed the queue are
        # can be destructive (JsonDumpStorage.destructiveDateWalk), we want to 
        # limit the damage in case of error or quit.
        self.worker_pool = thr.TaskManager(self.config.numberOfThreads,
                                           self.config.numberOfThreads * 2)
        self.quit = False
        self.logger.debug('finished init')

    #--------------------------------------------------------------------------
    def _quit_check(self):
        if self.quit:
            raise KeyboardInterrupt

    #--------------------------------------------------------------------------
    def _responsive_sleep (self, seconds, waitLogInterval=0, waitReason=''):
        for x in xrange(int(seconds)):
            self._quit_check()
            if waitLogInterval and not x % waitLogInterval:
                self.logger.info('%s: %dsec of %dsec',
                                 waitReason,
                                 x,
                                 seconds)
            time.sleep(1.0)

    #--------------------------------------------------------------------------
    def _responsive_join(self, thread, waitingFunc=None):
        while True:
            try:
                thread.join(1.0)
                if not thread.isAlive():
                    #self.logger.debug('%s is dead', str(thread))
                    break
                if waitingFunc:
                    waitingFunc()
            except KeyboardInterrupt:
                self.logger.debug ('quit detected by responsiveJoin')
                self.quit = True

    #--------------------------------------------------------------------------
    def blocking_start (self):
        try:
            self.start()
            self.waitForCompletion() # though, it only ends if someone hits
                                        # ^C or sends SIGHUP or SIGTERM - any of
                                        # which will get translated into a
                                        # KeyboardInterrupt exception
        except KeyboardInterrupt:
            while True:
                try:
                    submissionMill.stop()
                    break
                except KeyboardInterrupt:
                    logger.warning('We heard you the first time.  There is no '
                                   'need for further keyboard or signal '
                                   'interrupts.  We are waiting for the '
                                   'worker threads to stop.  If this app '
                                   'does not halt soon, you may have to send '
                                   'SIGKILL (kill -9)')

    #--------------------------------------------------------------------------
    def start (self):
        self.logger.debug('start')
        self.queuing_thread = threading.Thread(name="QueuingThread",
                                               target=self._queuing_thread_func)
        self.queuing_thread.start()

    #--------------------------------------------------------------------------
    def wait_for_completion (self, waitingFunc=None):
        self.logger.debug("waiting to join queuingThread")
        self._responsive_join(self.queuing_thread, waitingFunc)

    #--------------------------------------------------------------------------
    def stop (self):
        self.quit = True
        self.wait_for_completion()

    #--------------------------------------------------------------------------
    def _get_iterator(self):
        try:
            return self.jobSourceIterator(self.confg)
        except TypeError:
            try:
                return self.jobSourceIterator()
            except TypeError:
                return self.jobSourceIterator

    #--------------------------------------------------------------------------
    def _queuing_thread_func (self):
        self.logger.debug('queuingThreadFunc start')
        try:
            try:
                for aJob in self._get_iterator: # may never raise 
                                                # StopIteration
                    if aJob is None:
                        self.logger.info("there is nothing to do.  Sleeping "
                                         "for %d seconds" % 
                                         self.config.idle_delay)
                        self._responsive_sleep(self.config.idle_delay)
                        continue
                    self._quit_check()
                    try:
                        self.logger.debug("queuing standard job %s", aJob)
                        self.worker_pool.newTask(self.task_func, 
                                                 (aJob,))
                    except Exception:
                        self.logger.warning('%s has failed', aJob)
                        sutil.reportExceptionAndContinue(self.logger)
            except Exception:
                self.logger.warning('The jobSourceIterator has failed')
                sutil.reportExceptionAndContinue(self.logger)
            except KeyboardInterrupt:
                self.logger.debug('queuingThread gets quit request')
        finally:
            self.quit = True
            self.logger.debug("we're quitting queuingThread")
            self.logger.debug("waiting for standard worker threads to stop")
            self.worker_pool.waitForCompletion()
            self.logger.debug("all worker threads stopped")


#==============================================================================
class IteratorWorkerFrameworkWithRetry(IteratorWorkerFramework):
    """ """

    #--------------------------------------------------------------------------
    def __init__ (self, config, 
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
                                       "waiting for retry after failure in task")
        except KeyboardInterrupt:
            return




