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
from socorro.external.hbase.crashstorage import (
  CrashStorageSystemForHBase, DualHbaseCrashStorageSystem,
  CollectorCrashStorageSystemForHBase)
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


class TestHBaseCrashStorage(unittest.TestCase):


    def setUp(self):
        raise WorkHarderError
