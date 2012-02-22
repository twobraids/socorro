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
from socorro.external.filesystem.crashstorage import (
  CrashStorageForLocalFS, CrashStorageForNFS)
from configman import Namespace, ConfigurationManager


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


class TestLocalFSCrashStorage(unittest.TestCase):

    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.tmp_dir_fallback = tempfile.mkdtemp('fallback')

    def tearDown(self):
        shutil.rmtree(self.tmp_dir)
        shutil.rmtree(self.tmp_dir_fallback)

    @staticmethod
    def _get_class_methods(klass):
        return dict((n, ref) for (n, ref)
                    in inspect.getmembers(klass, inspect.ismethod)
                    if not n.startswith('_') and n in klass.__dict__)

    def test_abstract_classism(self):
        # XXX work in progress, might change prints ot asserts
        interface = self._get_class_methods(CrashStorageBase)
        implementor = self._get_class_methods(CrashStorageForLocalFS)
        for name in interface:
            if name not in implementor:
                print CrashStorageForLocalFS.__name__, "doesn't implement", name

    def _find_file(self, in_, filename):
        found = []
        for f in os.listdir(in_):
            path = os.path.join(in_, f)
            if os.path.isdir(path):
                found.extend(self._find_file(path, filename))
            elif os.path.isfile(path) and filename in path:
                found.append(path)
        return found

    def test_basic_localfs_crashstorage(self):

        mock_logging = MockLogging()
        required_config = CrashStorageForLocalFS.required_config
        required_config.add_option('logger', default=mock_logging)

        config_manager = ConfigurationManager(
          [required_config],
          app_name='testapp',
          app_version='1.0',
          app_description='app description',
          values_source_list=[{
            'logger': mock_logging,
            'local_fs': self.tmp_dir,
            'fallback_fs': self.tmp_dir_fallback,
          }]
        )
        with config_manager.context() as config:
            crashstorage = CrashStorageForLocalFS(config)
            self.assertEqual(list(crashstorage.new_ooids()), [])
            raw = '{"name": "Peter"}'
            self.assertRaises(
              OOIDNotFoundException,
              crashstorage.save_raw,
              json.loads(raw),
              raw
            )
            # deliberately write it differently from how json.dumps
            # would print it (no space after comma)
            raw = '{"name":"Peter","ooid":"abc123"}'
            result = crashstorage.save_raw(json.loads(raw), raw)
            self.assertEqual(result, CrashStorageForLocalFS.OK)
            msg, __ = config.logger.messages['info'][0]
            self.assertTrue('saved' in msg)
            self.assertTrue('abc123' in msg)

            self.assertEqual(
                len(self._find_file(self.tmp_dir, 'abc123.json')), 2)
            self.assertTrue(self._find_file(self.tmp_dir, 'abc123.dump'))
            self.assertTrue(self._find_file(self.tmp_dir, 'abc123.json'))
            self.assertTrue(
                not self._find_file(self.tmp_dir_fallback, 'abc123.json'))

            meta = crashstorage.get_raw_json('abc123')
            assert isinstance(meta, dict)
            self.assertEqual(meta['name'], 'Peter')

            dump = crashstorage.get_raw_dump('abc123')
            assert isinstance(dump, basestring)
            self.assertTrue('"name":"Peter"' in dump)

            # XXX Lars? Is this correct?
            self.assertRaises(NotImplementedError,
                              crashstorage.has_ooid,
                              'abc123')
            #self.assertEqual(crashstorage.has_ooid('abc123'), True)
            #self.assertEqual(crashstorage.has_ooid('abc000'), False)

            crashstorage.remove('abc123')
            self.assertTrue(not self._find_file(self.tmp_dir, 'abc123.json'))
            self.assertTrue(not self._find_file(self.tmp_dir, 'abc123.dump'))
