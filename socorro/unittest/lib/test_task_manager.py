# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import time
import unittest

from mock import Mock

from  socorro.lib.task_manager import TaskManager, default_task_func
from socorro.lib.util import DotDict, SilentFakeLogger


class TestTaskManager(unittest.TestCase):

    def setUp(self):
        self.logger = SilentFakeLogger()

    def tearDown(self):
        pass

    def test_constuctor1(self):
        config = DotDict()
        config.logger = self.logger
        tm = TaskManager(config)
        self.assertTrue(tm.config == config)
        self.assertTrue(tm.logger == self.logger)
        self.assertTrue(tm.task_func == default_task_func)
        self.assertTrue(tm.quit == False)

    def test_executor_identity(self):
        config = DotDict()
        config.logger = self.logger
        tm = TaskManager(
            config,
            job_source_iterator=range(1),

        )
        self.assertEqual(tm.executor_identity(), 'MainThread')

    def test_get_iterator(self):
        config = DotDict()
        config.logger = self.logger
        tm = TaskManager(
            config,
            job_source_iterator=range(1),
        )
        self.assertEqual(tm._get_iterator(), [0])

        def an_iter(self):
            for i in range(5):
                yield i

        tm = TaskManager(
            config,
            job_source_iterator=an_iter,
        )
        self.assertEqual(
            [x for x in tm._get_iterator()],
            [0, 1, 2, 3, 4]
        )

        class X(object):
            def __init__(self, config):
                self.config = config

            def __iter__(self):
                for key in self.config:
                    yield key

        tm = TaskManager(
            config,
            job_source_iterator=X(config)
        )
        self.assertEqual(
            [x for x in tm._get_iterator()],
            [y for y in config.keys()]
        )



