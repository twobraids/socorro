import unittest
import mock

from configman.dotdict import DotDict

from socorro.processor.legacy import (
  LegacyOoidSource,
)

def sequencer(*args):
    active_iter = iter(args)
    def foo(*args, **kwargs):
        try:
            value = active_iter.next()
        except StopIteration:
            raise Exception('out of values')
        if isinstance(value, Exception):
            raise value
        return value
    return foo

class TestPostgresCrashStorage(unittest.TestCase):
    """
    Tests where the urllib part is mocked.
    """

    def test_legacy_ooid_source_basics(self):
        m_transaction_executor_class = mock.Mock()

        config = DotDict()
        config.database_class = mock.Mock()
        config.transaction_executor_class = m_transaction_executor_class
        config.batchJobLimit = 10

        ooid_source = LegacyOoidSource(config,
                                       processor_name='dwight-1234')

        self.assertEqual(m_transaction_executor_class.call_count, 1)
        m_transaction_executor_class.assert_called_with(config)



    def test_incoming_job_stream_normal(self):
        config = DotDict()
        config.database_class = mock.Mock()
        config.transaction_executor_class = mock.Mock()
        config.batchJobLimit = 10
        config.logger = mock.Mock()

        class StubbedIterators(LegacyOoidSource):
            def newPriorityJobsIter(self):
                while True:
                    yield None

            def newNormalJobsIter(self):
                values = [
                    (1, '1234', 1),
                    (2, '2345', 1),
                    (3, '3456', 1),
                    (4, '4567', 1),
                    (5, '5678', 1),
                ]
                for x in values:
                    yield x

        ooid_source = StubbedIterators(config,
                                       processor_name='sherman1234')
        expected = ('1234',
                    '2345',
                    '3456',
                    '4567',
                    '5678',
                   )
        for x, y in zip(ooid_source, expected):
            self.assertEqual(x, y)

        self.assertEqual(len([x for x in ooid_source]), 5)


    def test_incoming_job_stream_priority(self):
        config = DotDict()
        config.database_class = mock.Mock()
        config.transaction_executor_class = mock.Mock()
        config.batchJobLimit = 10
        config.logger = mock.Mock()

        class StubbedIterators(LegacyOoidSource):
            def newNormalJobsIter(self):
                while True:
                    yield None

            def newPriorityJobsIter(self):
                values = [
                    (1, '1234', 1),
                    (2, '2345', 1),
                    (3, '3456', 1),
                    (4, '4567', 1),
                    (5, '5678', 1),
                ]
                for x in values:
                    yield x

        ooid_source = StubbedIterators(config,
                                       processor_name='sherman1234')
        expected = ('1234',
                    '2345',
                    '3456',
                    '4567',
                    '5678',
                   )
        for x, y in zip(ooid_source, expected):
            self.assertEqual(x, y)

        self.assertEqual(len([x for x in ooid_source]), 5)

    def test_incoming_job_stream_interleaved(self):
        config = DotDict()
        config.database_class = mock.Mock()
        config.transaction_executor_class = mock.Mock()
        config.batchJobLimit = 10
        config.logger = mock.Mock()

        class StubbedIterators(LegacyOoidSource):
            def newPriorityJobsIter(self):
                values = [
                    None,
                    (10, 'p1234', 1),
                    (20, 'p2345', 1),
                    None,
                    (30, 'p3456', 1),
                    (40, 'p4567', 1),
                    None,
                    None,
                    (50, 'p5678', 1),
                    None,
                ]
                for x in values:
                    yield x

            def newNormalJobsIter(self):
                values = [
                    (1, '1234', 1),
                    (2, '2345', 1),
                    (3, '3456', 1),
                    (4, '4567', 1),
                    (5, '5678', 1),
                    None,
                    None,
                ]
                for x in values:
                    yield x

        ooid_source = StubbedIterators(config,
                                       processor_name='sherman1234')
        expected = ('1234',
                    'p1234',
                    'p2345',
                    '2345',
                    'p3456',
                    'p4567',
                    '3456',
                    '4567',
                    'p5678',
                    '5678',
                   )
        for x, y in zip(ooid_source, expected):
            self.assertEqual(x, y)

        self.assertEqual(len([x for x in ooid_source]), 10)
