from  socorro.lib.threaded_task_manager import ThreadedTaskManager, OK, \
      default_task_func
from socorro.lib.util import DotDict, SilentFakeLogger
import socorro.lib.util as sutil

import time
import functools

def testConstuctor1 ():
    logger = SilentFakeLogger()
    config = DotDict({ 'logger': logger,
                       'number_of_threads': 1,
                       'maximum_queue_size': 1,
                     })
    ttm = ThreadedTaskManager(config)
    try:
        assert ttm.config == config
        assert ttm.logger == logger
        assert ttm.task_func == default_task_func
        assert ttm.quit == False
    finally:
        # we got threads to join
        ttm._kill_worker_threads()

def testStart1 ():
    logger = SilentFakeLogger()
    config = DotDict({ 'logger': logger,
                       'number_of_threads': 1,
                       'maximum_queue_size': 1,
                     })
    ttm = ThreadedTaskManager(config)
    try:
        ttm.start()
        time.sleep(2.0)
        assert ttm.queuing_thread.isAlive(), "the queing thread is not running"
        assert len(ttm.thread_list) == 1, "where's the worker thread?"
        assert ttm.thread_list[0].isAlive(), "the worker thread is stillborn"
        ttm.stop()
        assert ttm.queuing_thread.isAlive() == False, "the queuing thread did not stop"
    except Exception:
        # we got threads to join
        ttm.wait_for_completion()

def testDoingWorkWithOneWorker():
    logger = SilentFakeLogger()
    config = DotDict({ 'logger': logger,
                       'number_of_threads': 1,
                       'maximum_queue_size': 1,
                     })
    my_list = []
    def insert_into_list(anItem):
        my_list.append(anItem[0])
        return OK
    ttm = ThreadedTaskManager(config,
                              task_func=insert_into_list
                             )
    try:
        ttm.start()
        time.sleep(2.0)
        assert len(my_list) == 10, 'expected to do 10 inserts, but %d were done instead' % len(my_list)
        assert my_list == range(10), 'expected %s, but got %s' % (range(10), my_list)
        ttm.stop()
    except Exception:
        # we got threads to join
        ttm.wait_for_completion()
        raise

def testDoingWorkWithTwoWorkersAndGenerator():
    logger = SilentFakeLogger()
    config = DotDict({ 'logger': logger,
                       'number_of_threads': 2,
                       'maximum_queue_size': 2,
                     })
    my_list = []
    def insert_into_list(anItem):
        my_list.append(anItem[0])
        return OK
    ttm = ThreadedTaskManager(config,
                              task_func=insert_into_list,
                              job_source_iterator=(x for x in
                                                     xrange(10))
                             )
    try:
        ttm.start()
        time.sleep(2.0)
        assert len(ttm.thread_list) == 2, "expected 2 threads, but found %d" % len(ttm.thread_list)
        assert len(my_list) == 10, 'expected to do 10 inserts, but %d were done instead' % len(my_list)
        assert sorted(my_list) == range(10), 'expected %s, but got %s' % (range(10), sorted(my_list))
    except Exception:
        # we got threads to join
        ttm.worker_pool.wait_for_completion()
        raise
            
        
def testDoingWorkWithTwoWorkersAndConfigSetup():
    def new_iter():
        for x in xrange(5):
            yield x
    
    my_list = []
    def insert_into_list(anItem):
        my_list.append(anItem[0])
        return OK
    
    logger = SilentFakeLogger()
    config = DotDict({ 'logger': logger,
                       'number_of_threads': 2,
                       'maximum_queue_size': 2,
                       'job_source_iterator': new_iter,
                       'task_func': insert_into_list
                     })
    ttm = ThreadedTaskManager(config)
    try:
        ttm.start()
        time.sleep(2.0)
        assert len(ttm.thread_list) == 2, "expected 2 threads, but found %d" % len(ttm.thread_list)
        assert len(my_list) == 5, 'expected to do 5 inserts, but %d were done instead' % len(my_list)
        assert sorted(my_list) == range(5), 'expected %s, but got %s' % (range(5), sorted(my_list))
    except Exception:
        # we got threads to join
        ttm.wait_for_completion()
        raise
        
# failure tests

count = 0

def testTaskRaisesUnexpectedException():
    def new_iter():
        for x in xrange(10):
            yield x
    
    my_list = []
    def insert_into_list(anItem):
        global count
        count += 1
        if count == 4:
            raise Exception('Unexpected')
        my_list.append(anItem[0])
        return OK
    
    logger = SilentFakeLogger()
    config = DotDict({ 'logger': logger,
                       'number_of_threads': 1,
                       'maximum_queue_size': 1,
                       'job_source_iterator': new_iter,
                       'task_func': insert_into_list
                     })
    ttm = ThreadedTaskManager(config)
    try:
        ttm.start()
        time.sleep(2.0)
        assert len(ttm.thread_list) == 1, "expected 1 threads, but found %d" % len(ttm.thread_list)
        #assert ttm.threadList[0].isAlive(), "1st thread is unexpectedly dead"
        #assert ttm.threadList[1].isAlive(), "2nd thread is unexpectedly dead"
        assert sorted(my_list) == [0, 1, 2, 4, 5, 6, 7, 8, 9], 'expected %s, but got %s' % ( [0, 1, 2, 5, 6, 7, 8, 9], sorted(my_list))
        assert len(my_list) == 9, 'expected to do 9 inserts, but %d were done instead' % len(my_list)
    except Exception:
        # we got threads to join
        ttm.wait_for_completion()
        raise
            
