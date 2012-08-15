from configman import RequiredConfig, Namespace
from socorro.database.transaction_executor import TransactionExecutor

required_config = Namespace()
required_config.add_option('transaction_executor_class',
                           default=TransactionExecutor,
                           doc='a class that will manage transactions')
required_config.add_option('submission_url',
                           doc='a url to submit crash_ids for Elastic '
                           'Search '
                           '(use %s in place of the crash_id) '
                           '(leave blank to disable)',
                           default='')
required_config.add_option('timeout',
                           doc='how long to wait in seconds for '
                               'confirmation of a submission',
                           default=2)

