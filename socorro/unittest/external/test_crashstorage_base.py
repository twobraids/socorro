import unittest
from socorro.external.crashstorage_base import CrashStorageBase
from configman import Namespace, ConfigurationManager
from mock import Mock


class TestBase(unittest.TestCase):

    def test_basic_crashstorage(self):

        required_config = Namespace()

        mock_logging = Mock()
        required_config.add_option('logger', default=mock_logging)

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
            crashstorage.save_raw_crash({}, 'payload')
            crashstorage.save_processed({})
            self.assertRaises(NotImplementedError,
                              crashstorage.get_raw_json, 'ooid')
            self.assertRaises(NotImplementedError,
                              crashstorage.get_raw_dump, 'ooid')
            self.assertRaises(NotImplementedError,
                              crashstorage.get_processed_json, 'ooid')
            self.assertRaises(NotImplementedError,
                              crashstorage.remove, 'ooid')
            self.assertRaises(StopIteration, crashstorage.new_ooids)
            self.assertRaises(NotImplementedError, crashstorage.close)
