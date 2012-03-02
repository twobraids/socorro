import os
import functools
import json
import shutil
import tempfile
import inspect
from pprint import pprint
import unittest
from socorro.external.crashstorage_base import (
  CrashStorageBase, OOIDNotFoundException)
from socorro.external.hbase.crashstorage import HBaseCrashStorage
from socorro.storage import hbaseClient
from configman import Namespace, ConfigurationManager
from socorro.unittest.config import commonconfig


class MockLogging(object):

    def __init__(self):
        self.messages = {}
        for each in 'info', 'debug', 'warning', 'error', 'critical':
            self.messages[each] = []

    def _append(self, bucket, args, kwargs):
        msg = args[0] % tuple(args[1:])
        self.messages[bucket].append((msg, kwargs))

    def info(self, *args, **kwargs):
        self._append('info', args, kwargs)

    def debug(self, *args, **kwargs):
        self._append('debug', args, kwargs)

    def warning(self, *args, **kwargs):
        self._append('warning', args, kwargs)

    def error(self, *args, **kwargs):
        self._append('error', args, kwargs)

    def critical(self, *args, **kwargs):
        self._append('critical', args, kwargs)


class TestHBaseCrashStorage(unittest.TestCase):
    """
    If you ever get this::
        Traceback (most recent call last):
        ...
        socorro.storage.hbaseClient.FatalException: the connection is not viable.  retries fail:
    
    Then try the following:
        
        /etc/init.d/hadoop-hbase-master restart
        /etc/init.d/hadoop-hbase-thrift restart
        
    Also, you can look in /var/log/hbase for clues. 
    Still not working, try:
        
        hbase shell
        > describe 'crash_reports'
        
    and keep an eye on the logs. 
    """

    def setUp(self):
        connection = hbaseClient.HBaseConnectionForCrashReports(
            commonconfig.hbaseHost.default,
            commonconfig.hbasePort.default,
            100
        )
        for row in connection.merge_scan_with_prefix(
          'crash_reports', '', ['ids:ooid']):
            index_row_key = row['_rowkey']
            connection.client.deleteAllRow(
              'crash_reports', index_row_key)
              
    @staticmethod
    def _get_class_methods(klass):
        return dict((n, ref) for (n, ref)
                    in inspect.getmembers(klass, inspect.ismethod)
                    if not n.startswith('_') and n in klass.__dict__)

    def test_abstract_classism(self):
        # XXX work in progress, might change prints ot asserts
        interface = self._get_class_methods(CrashStorageBase)
        implementor = self._get_class_methods(HBaseCrashStorage)
        for name in interface:
            if name not in implementor:
                print HBaseCrashStorage.__name__, "doesn't implement", name
                
    def test_basic_hbase_crashstorage(self):
        mock_logging = MockLogging()
        required_config = HBaseCrashStorage.required_config
        required_config.add_option('logger', default=mock_logging)
        
        config_manager = ConfigurationManager(
          [required_config],
          app_name='testapp',
          app_version='1.0',
          app_description='app description',
          values_source_list=[{
            'logger': mock_logging,
            'hbase_timeout': 100,
            'hbase_host': commonconfig.hbaseHost.default,
            'hbase_port': commonconfig.hbasePort.default,
          }]
        )
        with config_manager.context() as config:
            crashstorage = HBaseCrashStorage(config)
            self.assertEqual(list(crashstorage.new_ooids()), [])
            
            # data doesn't contain an 'ooid' key
            raw = '{"name": "Peter"}'
            self.assertRaises(
              OOIDNotFoundException,
              crashstorage.save_raw,
              json.loads(raw),
              raw
            )

            raw = '{"name":"Peter","ooid":"abc123"}'
            self.assertRaises(
              ValueError,  # missing the 'submitted_timestamp' key
              crashstorage.save_raw,
              json.loads(raw),
              raw
            )
            import time
            raw = ('{"name":"Peter","ooid":"abc123",'
                   '"submitted_timestamp":"%d"}' % time.time())
            result = crashstorage.save_raw(json.loads(raw), raw)
            self.assertEqual(result, CrashStorageBase.OK)
            msg, __ = config.logger.messages['info'][1]
            self.assertTrue('saved' in msg)
            self.assertTrue('abc123' in msg)
            
            meta = crashstorage.get_raw_json('abc123')
            assert isinstance(meta, dict)
            self.assertEqual(meta['name'], 'Peter')

            dump = crashstorage.get_raw_dump('abc123')
            assert isinstance(dump, basestring)
            self.assertTrue('"name":"Peter"' in dump)

            self.assertTrue(crashstorage.has_ooid('abc123'))
            # call it again, just to be sure
            self.assertTrue(crashstorage.has_ooid('abc123'))
            self.assertTrue(not crashstorage.has_ooid('xyz789'))
            
            # hasn't been processed yet
            self.assertRaises(hbaseClient.OoidNotFoundException,
                              crashstorage.get_processed, 
                              'abc123')
            
            raw = ('{"name":"Peter","ooid":"abc123", '
                   '"submitted_timestamp":"%d", '
                   '"completeddatetime": "%d"}' % 
                   (time.time(), time.time()))
            crashstorage.save_processed('abc123', json.loads(raw))
            data = crashstorage.get_processed('abc123')
            self.assertEqual(data['name'], u'Peter')

            

            
            

     
    
