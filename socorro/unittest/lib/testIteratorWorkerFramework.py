import socorro.lib.iteratorWorkerFramework as siwf
import socorro.lib.util as sutil

import time
import functools

def testConstuctor1 ():
    logger = sutil.SilentFakeLogger()
    config = sutil.DotDict({ 'logger': logger,
                             'numberOfThreads': 1
                           })
    iwf = siwf.IteratorWorkerFramework(config)
    try:
        assert iwf.config == config
        assert iwf.logger == logger
        assert iwf.task_func == siwf.default_task_func
        assert iwf.quit == False
    finally:
        # we got threads to join
        iwf.worker_pool.waitForCompletion()

def testStart1 ():
    logger = sutil.SilentFakeLogger()
    config = sutil.DotDict({ 'logger': logger,
                             'numberOfThreads': 1
                           })
    iwf = siwf.IteratorWorkerFramework(config)
    try:
        iwf.start()
        time.sleep(2.0)
        assert iwf.queuing_thread.isAlive(), "the queing thread is not running"
        assert len(iwf.worker_pool.threadList) == 1, "where's the worker thread?"
        assert iwf.worker_pool.threadList[0].isAlive(), "the worker thread is stillborn"
        iwf.stop()
        assert iwf.queuing_thread.isAlive() == False, "the queuing thread did not stop"
    except Exception:
        # we got threads to join
        iwf.worker_pool.waitForCompletion()

def testDoingWorkWithOneWorker():
    logger = sutil.SilentFakeLogger()
    config = sutil.DotDict({ 'logger': logger,
                             'numberOfThreads': 1
                           })
    my_list = []
    def insert_into_list(anItem):
        my_list.append(anItem[0])
        return siwf.OK
    iwf = siwf.IteratorWorkerFramework(config,
                                       task_func=insert_into_list
                                      )
    try:
        iwf.start()
        time.sleep(2.0)
        assert len(my_list) == 10, 'expected to do 10 inserts, but %d were done instead' % len(my_list)
        assert my_list == range(10), 'expected %s, but got %s' % (range(10), my_list)
        iwf.stop()
    except Exception:
        # we got threads to join
        iwf.worker_pool.waitForCompletion()
        raise

def testDoingWorkWithTwoWorkersAndGenerator():
    logger = sutil.SilentFakeLogger()
    config = sutil.DotDict({ 'logger': logger,
                             'numberOfThreads': 2
                           })
    my_list = []
    def insert_into_list(anItem):
        my_list.append(anItem[0])
        return siwf.OK
    iwf = siwf.IteratorWorkerFramework(config,
                                       task_func=insert_into_list,
                                       job_source_iterator=(x for x in
                                                              xrange(10))
                                      )
    try:
        iwf.start()
        time.sleep(2.0)
        assert len(iwf.worker_pool.threadList) == 2, "expected 2 threads, but found %d" % len(iwf.worker_pool.threadList)
        assert len(my_list) == 10, 'expected to do 10 inserts, but %d were done instead' % len(my_list)
        assert sorted(my_list) == range(10), 'expected %s, but got %s' % (range(10), sorted(my_list))
    except Exception:
        # we got threads to join
        iwf.worker_pool.waitForCompletion()
        raise
            
        
def testDoingWorkWithTwoWorkersAndConfigSetup():
    def new_iter():
        for x in xrange(5):
            yield x
    
    my_list = []
    def insert_into_list(anItem):
        my_list.append(anItem[0])
        return siwf.OK
    
    logger = sutil.SilentFakeLogger()
    config = sutil.DotDict({ 'logger': logger,
                             'numberOfThreads': 2,
                             'job_source_iterator': new_iter,
                             'task_func': insert_into_list
                           })
    iwf = siwf.IteratorWorkerFramework(config)
    try:
        iwf.start()
        time.sleep(2.0)
        assert len(iwf.worker_pool.threadList) == 2, "expected 2 threads, but found %d" % len(iwf.worker_pool.threadList)
        assert len(my_list) == 5, 'expected to do 5 inserts, but %d were done instead' % len(my_list)
        assert sorted(my_list) == range(5), 'expected %s, but got %s' % (range(5), sorted(my_list))
    except Exception:
        # we got threads to join
        iwf.worker_pool.waitForCompletion()
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
        return siwf.OK
    
    logger = sutil.SilentFakeLogger()
    config = sutil.DotDict({ 'logger': logger,
                             'numberOfThreads': 1,
                             'job_source_iterator': new_iter,
                             'task_func': insert_into_list
                           })
    iwf = siwf.IteratorWorkerFramework(config)
    try:
        iwf.start()
        time.sleep(2.0)
        assert len(iwf.worker_pool.threadList) == 1, "expected 1 threads, but found %d" % len(iwf.worker_pool.threadList)
        #assert iwf.worker_pool.threadList[0].isAlive(), "1st thread is unexpectedly dead"
        #assert iwf.worker_pool.threadList[1].isAlive(), "2nd thread is unexpectedly dead"
        assert sorted(my_list) == [0, 1, 2, 4, 5, 6, 7, 8, 9], 'expected %s, but got %s' % ( [0, 1, 2, 5, 6, 7, 8, 9], sorted(my_list))
        assert len(my_list) == 9, 'expected to do 9 inserts, but %d were done instead' % len(my_list)
    except Exception:
        # we got threads to join
        iwf.worker_pool.waitForCompletion()
        raise
            
