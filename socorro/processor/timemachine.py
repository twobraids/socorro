from datetime import timedelta

from socorro.lib.transform_rules import Rule
from socorro.external.postgresql.dbapi2_util import execute_query_fetchall

from configman import Namespace, RequiredConfig
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
        'how much to change the date_processed into the past',
        default='0 08:00:00',  # 8 hours
        from_string_converter=str_to_timedelta
    )

    #--------------------------------------------------------------------------
    def __init__(self, config):
        self.crashstore = config.crashstorage_class(config)

    #--------------------------------------------------------------------------
    def _action(self, raw_crash, raw_dumps, processed_crash, processor_meta):
        crash_id = raw_crash.uuid
        old_processed_crash = self.crashstore.get_unredacted_processed(crash_id)
        for key, value in old_processed_crash.iteritems():
            if 'date_processed' in key:
                processed_crash[key] = value - self.config.time_delta
            else:
                processed_crash[key] = value
        return True


#==============================================================================
class PGQueryNewCrashSource(RequiredConfig):
    required_config = Namespace()
    required_config.add_option(
        'crashstorage_class',
        doc='the source storage class',
        default='socorro.external.postgresql.crashstorage.PostgreSQLCrashStorage',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'crash_id_query',
        doc='sql to get a list of crash_ids',
        default="select uuid from reports where uuid like '%142022' and date_processed > '2014-10-23'",
        from_string_converter=class_converter
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, processor_name, quit_check_callback=None):
        self.crash_store = config.crashstorage_class(
            config,
            quit_check_callback
        )


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
            yield (a_crash_id,)
            

    #--------------------------------------------------------------------------
    def __call__(self):
        return self.__iter__()
