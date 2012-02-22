import configman
import sys

import unittest
from socorro.external.crashstorage_base import CrashStorageBase
from configman import Namespace, ConfigurationManager


class MockLogging:

    def __init__(self):
        self.debugs = []
        self.warnings = []
        self.errors = []

    def debug(self, *args, **kwargs):
        self.debugs.append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.warnings.append((args, kwargs))

    def error(self, *args, **kwargs):
        self.errors.append((args, kwargs))


class TestBase(unittest.TestCase):

    def test_basic_crashstorage(self):

        required_config = Namespace()
        mock_logging = MockLogging()
        XXX
#        required_config.add_option('logger', default=mock_logging)

        config_manager = ConfigurationManager(
          [required_config],
          app_name='testapp',
          app_version='1.0',
          app_description='app description',
          values_source_list=[{
            'logger': mock_logging,
          }]
        )

        with config_manager.context() as config:
            crashstorage = CrashStorageBase(config)
            self.assertEqual(crashstorage.save_raw({}, 'payload'),
                             CrashStorageBase.NO_ACTION)
            self.assertEqual(crashstorage.save_processed({}),
                             CrashStorageBase.NO_ACTION)

            self.assertRaises(NotImplementedError,
                              crashstorage.get_raw_json, 'ooid')
            self.assertRaises(NotImplementedError,
                              crashstorage.get_raw_dump, 'ooid')
            self.assertRaises(NotImplementedError,
                              crashstorage.get_processed_json, 'ooid')
            self.assertRaises(NotImplementedError,
                              crashstorage.remove, 'ooid')
            self.assertTrue(not crashstorage.has_ooid('anything'))
            self.assertRaises(StopIteration, crashstorage.new_ooids)
