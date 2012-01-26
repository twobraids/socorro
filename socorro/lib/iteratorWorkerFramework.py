import signal
import time
import threading


#logger = logging.getLogger("base")

import socorro.lib.util as sutil
import socorro.lib.threadlib as thr

from configman import RequiredConfig, Namespace
from configman.converters import ClassConverter

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
    signame = 'SIGTERM'
    if signalNumber != signal.SIGTERM:
        signame = 'SIGHUP'
    #self.logger.info("%s detected", signame)
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
                               from_string_converter=ClassConverter)
    required_config.add_option('task_func',
                               default=default_task_func,
                               doc='a callable that accomplishes a task',
                               from_string_converter=ClassConverter)

    #--------------------------------------------------------------------------
    def __init__ (self, config, job_source_iterator=default_iterator,
                  task_func=default_task_func):
        super(IteratorWorkerFramework, self).__init__()
        self.config = config
        self.logger = config.logger
        self.jobSourceIterator = config.setdefault('job_source_iterator',
                                                   job_source_iterator)
        self.task_func = config.setdefault('taskFunc', task_func)
        
        # setup the task manager to a queue size twice the size of the number
        # of threads in use.  Because some mechanisms that feed the queue are
        # can be destructive (JsonDumpStorage.destructiveDateWalk), we want to 
        # limit the damage in case of error or quit.
        self.workerPool = thr.TaskManager(self.config.numberOfThreads,
                                          self.config.numberOfThreads * 2)
        self.quit = False
        self.logger.debug('finished init')

    #--------------------------------------------------------------------------
    def quit_check(self):
        if self.quit:
            raise KeyboardInterrupt

    #--------------------------------------------------------------------------
    def responsive_sleep (self, seconds, waitLogInterval=0, waitReason=''):
        for x in xrange(int(seconds)):
            self.quit_check()
            if waitLogInterval and not x % waitLogInterval:
                self.logger.info('%s: %dsec of %dsec',
                                 waitReason,
                                 x,
                                 seconds)
            time.sleep(1.0)

    #--------------------------------------------------------------------------
    def responsive_join(self, thread, waitingFunc=None):
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
    def start (self):
        self.logger.debug('start')
        self.queuingThread = threading.Thread(name="%sQueuingThread" % 
                                                   self.name,
                                              target=self.queuing_thread_func)
        self.queuingThread.start()

    #--------------------------------------------------------------------------
    def wait_for_completion (self, waitingFunc=None):
        self.logger.debug("waiting to join queuingThread")
        self.responsive_join(self.queuingThread, waitingFunc)

    #--------------------------------------------------------------------------
    def stop (self):
        self.quit = True
        self.wait_for_completion()

    #--------------------------------------------------------------------------
    def queuing_thread_func (self):
        self.logger.debug('queuingThreadFunc start')
        try:
            try:
                try:
                    job_iter = self.jobSourceIterator()
                except TypeError:
                    job_iter = self.jobSourceIterator
                for aJob in job_iter: # may never raise 
                                      # StopIteration
                    if aJob is None:
                        self.logger.info("there is nothing to do.  Sleeping "
                                         "for 7 seconds")
                        self.responsive_sleep(7)
                        continue
                    self.quit_check()
                    try:
                        self.logger.debug("queuing standard job %s", aJob)
                        self.workerPool.newTask(self.task_func, 
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
            self.workerPool.waitForCompletion()
            self.logger.debug("all worker threads stopped")





#==============================================================================
class IteratorWorkerFrameworkWithRetry(IteratorWorkerFramework):
    """ """
    
    #--------------------------------------------------------------------------
    def __init__ (self, config, 
                  name='mill', 
                  job_source_iterator=default_iterator,
                  taskFunc=default_task_func):
        """
        Note about 'jobSourceIterator': this is perhaps a design flaw.  It 
        isn't really an iterator.  It is a function that returns an iterator.  
        Just passing in an iterator that's already activated or a generator 
        expression will fail.
        """
        super(IteratorWorkerFrameworkWithRetry, self).__init__(config,
                                                        job_source_iterator,
                                                        taskFunc)
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
                self.responsive_sleep(waitInSeconds,
                                     10,
                                     "waiting for retry after failure in task")
        except KeyboardInterrupt:
            return





