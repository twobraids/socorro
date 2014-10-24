from datetime import timedelta, datetime

from socorro.lib.transform_rules import Rule
from socorro.external.postgresql.dbapi2_util import execute_query_fetchall
from socorro.lib.datetimeutil import string_to_datetime, date_to_string

from configman import Namespace, RequiredConfig, class_converter
from configman.converters import str_to_timedelta

#==============================================================================
class DateProcessedTimeMachine(Rule):
    required_config = Namespace()
    required_config.add_option(
        name='crashstorage_class',
        doc='the crash storage system class',
        default='socorro.external.hb.crashstorage.HBaseCrashStorage',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'time_delta',
        doc='how much to change the date_processed into the past',
        default='0 08:00:00',  # 8 hours
        from_string_converter=str_to_timedelta
    )

    #--------------------------------------------------------------------------
    def __init__(self, config):
        super(DateProcessedTimeMachine, self).__init__(config)
        self.crashstore = config.crashstorage_class(config)

    #--------------------------------------------------------------------------
    def _action(self, raw_crash, raw_dumps, processed_crash, processor_meta):
        crash_id = raw_crash.uuid
        old_processed_crash = self.crashstore.get_unredacted_processed(crash_id)

        for key, value in old_processed_crash.iteritems():
            if 'date_processed' in key:
                processed_crash[key] = date_to_string(
                    string_to_datetime(value) - self.config.time_delta
                )
                print processed_crash.uuid, value, processed_crash[key]
            else:
                if 'time' in key or "date" in key or 'Date' in key:
                    value = date_to_string(string_to_datetime(value))
                processed_crash[key] = value
        return True


#==============================================================================
class PGQueryNewCrashSource(RequiredConfig):
    required_config = Namespace()
    required_config.add_option(
        'crashstorage_class',
        doc='the source storage class',
        default='socorro.external.postgresql.crashstorage.PostgreSQLCrashStorage',
        from_string_converter=class_converter,
        reference_value_from='resource.postgresql'
    )
    required_config.add_option(
        'crash_id_query',
        doc='sql to get a list of crash_ids',
        default="select uuid from reports where uuid like '%142022' and date_processed > '2014-10-23'",
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, processor_name, quit_check_callback=None):
        self.crash_store = config.crashstorage_class(
            config,
            quit_check_callback
        )
        self.config = config


    #--------------------------------------------------------------------------
    def close(self):
        pass

    #--------------------------------------------------------------------------
    def __iter__(self):
        """Return an iterator over crashes from RabbitMQ.

        Each crash is a tuple of the ``(args, kwargs)`` variety. The lone arg
        is a crash ID, and the kwargs contain only a callback function which
        the FTS app will call to send an ack to Rabbit after processing is
        complete.

        """

        crash_ids = self.crash_store.transaction(
            execute_query_fetchall,
            self.config.crash_id_query
        )

        for a_crash_id in crash_ids:
            yield a_crash_id

        while True:
            yield None


    #--------------------------------------------------------------------------
    def __call__(self):
        return self.__iter__()


from ujson import dumps

from socorro.processor.processor_2015 import Processor2015

#------------------------------------------------------------------------------
time_machine_rule_set = [
    [   # rules to transform a raw crash into a processed crash
        "raw_to_processed_transform",
        "processer.raw_to_processed",
        "socorro.lib.transform_rules.TransformRuleSystem",
        "apply_all_rules",
        "socorro.processor.timemachine.DateProcessedTimeMachine"
    ],
]


#==============================================================================
class TimeMachineAlgorithm(Processor2015):
    """this is the class that processor uses to transform """

    Processor2015.required_config.rule_sets.set_default(
        dumps(time_machine_rule_set),
        force=True
    )

