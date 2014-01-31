import unittest

import mock

from configman import ConfigurationManager, RequiredConfig, Namespace
from configman.dotdict import DotDict

import socorro.cron.crontab_mixins as ctm

from socorro.external.postgresql.connection_context import ConnectionContext
from socorro.database.transaction_executor import TransactionExecutor


class FakeResourceClass(object):
    pass

class TestCrontabMixins(unittest.TestCase):

    def test_as_backfill_cron_app_simple_success(self):
        @ctm.as_backfill_cron_app
        class Alpha(object):
            pass
        a = Alpha()
        self.assertTrue(hasattr(a, 'main'))
        self.assertTrue(hasattr(Alpha, 'required_config'))

    def test_as_backfill_cron_app_main_overrides(self):
        @ctm.as_backfill_cron_app
        class Alpha(object):
            def main(self, function, once):
                return (function, once)
        a = Alpha()
        self.assertTrue(hasattr(a, 'main'))
        self.assertTrue(Alpha in Alpha.__mro__)
        self.assertTrue(Alpha is Alpha.__mro__[0])
        self.assertEqual(a.main(18), (18, False))

    def test_with_transactional_resource(self):
        @ctm.with_transactional_resource(
            'socorro.external.postgresql.connection_context.ConnectionContext',
            'database'
        )
        class Alpha(object):
            def __init__(self, config):
                self.config = config
        self.assertTrue
        self.assertTrue(hasattr(Alpha, "required_config"))
        self.assertTrue(RequiredConfig in Alpha.__mro__)
        alpha_required = Alpha.get_required_config()
        self.assertTrue(isinstance(alpha_required, Namespace))
        self.assertTrue('database' in alpha_required)
        self.assertTrue('database_class' in alpha_required.database)
        self.assertTrue(
            'database_transaction_executor_class' in alpha_required.database
        )
        cm = ConfigurationManager(
            definition_source=[Alpha.get_required_config(),],
            values_source_list=[],
            argv_source=[],
        )
        config = cm.get_config()
        a = Alpha(config)
        self.assertTrue(hasattr(a, 'database_connection'))
        self.assertTrue(isinstance(
            a.database_connection,
            ConnectionContext
        ))
        self.assertTrue(hasattr(a, 'database_transaction'))
        self.assertTrue(isinstance(
            a.database_transaction,
            TransactionExecutor
        ))

    def test_with_resource_connection_as_argument(self):
        @ctm.with_transactional_resource(
            'socorro.external.postgresql.connection_context.ConnectionContext',
            'database'
        )
        @ctm.with_resource_connection_as_argument('database')
        class Alpha(object):
            def __init__(self, config):
                self.config = config
        self.assertTrue(hasattr(Alpha, '_run_proxy'))

    def test_with_subprocess_mixin(self):
        @ctm.with_transactional_resource(
            'socorro.external.postgresql.connection_context.ConnectionContext',
            'database'
        )
        @ctm.with_single_transaction('database')
        @ctm.with_subprocess_mixin
        class Alpha(object):
            def __init__(self, config):
                self.config = config
        self.assertTrue(hasattr(Alpha, '_run_proxy'))
        self.assertTrue(hasattr(Alpha, 'run_process'))


    def test_with_postgres_transactions(self):
        @ctm.with_postgres_transactions()
        class Alpha(object):
            def __init__(self, config):
                self.config = config
        self.assertTrue
        self.assertTrue(hasattr(Alpha, "required_config"))
        self.assertTrue(RequiredConfig in Alpha.__mro__)
        alpha_required = Alpha.get_required_config()
        self.assertTrue(isinstance(alpha_required, Namespace))
        self.assertTrue('database' in alpha_required)
        self.assertTrue('database_class' in alpha_required.database)
        self.assertTrue(
            'database_transaction_executor_class' in alpha_required.database
        )

    def test_with_postgres_connection_as_argument(self):
        @ctm.with_postgres_transactions()
        @ctm.with_postgres_connection_as_argument()
        class Alpha(object):
            def __init__(self, config):
                self.config = config
        self.assertTrue(hasattr(Alpha, '_run_proxy'))
