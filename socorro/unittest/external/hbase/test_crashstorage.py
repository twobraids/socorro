import time
import json
import inspect
import unittest
from socorro.external.crashstorage_base import (
  CrashStorageBase, OOIDNotFoundException)
from socorro.external.hbase.crashstorage import HBaseCrashStorage
from socorro.storage import hbaseClient
from configman import ConfigurationManager
from socorro.unittest.config import commonconfig
import mock


class TestIntegrationHBaseCrashStorage(unittest.TestCase):
    """
    If you ever get this::
        Traceback (most recent call last):
        ...
        socorro.storage.hbaseClient.FatalException: the connection is not viab\
        le.  retries fail:

    Then try the following:

        /etc/init.d/hadoop-hbase-master restart
        /etc/init.d/hadoop-hbase-thrift restart

    Also, you can look in /var/log/hbase for clues.
    Still not working, try:

        hbase shell
        > describe 'crash_reports'

    and keep an eye on the logs.
    """

    def tearDown(self):
        self._truncate_hbase_table()

    def _truncate_hbase_table(self):
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
        # because of HBase's async nature, deleting can take time
        time.sleep(.1)

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
        mock_logging = mock.Mock()
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

            raw = ('{"name":"Peter","ooid":"abc123",'
                   '"submitted_timestamp":"%d"}' % time.time())
            result = crashstorage.save_raw(json.loads(raw), raw)
            self.assertEqual(result, CrashStorageBase.OK)

            assert config.logger.info.called
            assert config.logger.info.call_count > 1
            msg_tmpl, msg_arg = config.logger.info.call_args_list[1][0]
            # ie logging.info(<template>, <arg>)
            msg = msg_tmpl % msg_arg
            self.assertTrue('saved' in msg)
            self.assertTrue('abc123' in msg)

            meta = crashstorage.get_raw_json('abc123')
            assert isinstance(meta, dict)
            self.assertEqual(meta['name'], 'Peter')

            dump = crashstorage.get_raw_dump('abc123')
            assert isinstance(dump, basestring)
            self.assertTrue('"name":"Peter"' in dump)

            # hasn't been processed yet
            self.assertRaises(hbaseClient.OoidNotFoundException,
                              crashstorage.get_processed_json,
                              'abc123')

            raw = ('{"name":"Peter","ooid":"abc123", '
                   '"submitted_timestamp":"%d", '
                   '"completeddatetime": "%d"}' %
                   (time.time(), time.time()))

            crashstorage.save_processed('abc123', json.loads(raw))
            data = crashstorage.get_processed_json('abc123')
            self.assertEqual(data['name'], u'Peter')
            assert crashstorage.hbaseConnection.transport.isOpen()
            crashstorage.close()
            transport = crashstorage.hbaseConnection.transport
            self.assertTrue(not transport.isOpen())


class TestHBaseCrashStorage(unittest.TestCase):

    def test_basic_hbase_crashstorage(self):
        mock_logging = mock.Mock()
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
            hbaseclient_ = 'socorro.external.hbase.crashstorage.hbaseClient'
            with mock.patch(hbaseclient_) as hclient:

                class SomeThriftError(Exception):
                    pass

                instance = (hclient
                            .HBaseConnectionForCrashReports
                            .return_value)
                instance.hbaseThriftExceptions = (SomeThriftError,)

                def raiser(*args, **kwargs):
                    raise ValueError('shit!')

                instance.put_json_dump = raiser
                crashstorage = HBaseCrashStorage(config)
                assert hclient.HBaseConnectionForCrashReports.call_count == 1

                raw = ('{"name":"Peter","ooid":"abc123",'
                       '"submitted_timestamp":"%d"}' % time.time())
                result = crashstorage.save_raw(json.loads(raw), raw)
                self.assertEqual(result, CrashStorageBase.ERROR)
                args, kwargs = config.logger.error.call_args_list[-1]
                self.assertTrue(args)
                self.assertTrue(kwargs.get('exc_info'))

                def retry_raiser(*args, **kwargs):
                    raise SomeThriftError('try again')

                instance.put_json_dump = retry_raiser
                result = crashstorage.save_raw(json.loads(raw), raw)
                self.assertEqual(result, CrashStorageBase.RETRY)
