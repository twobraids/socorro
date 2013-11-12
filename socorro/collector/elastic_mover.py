import time

from configman import Namespace, RequiredConfig, class_converter

from socorro.app.fetch_transform_save_app import FetchTransformSaveApp, main

from socorro.external.postgresql.dbapi2_util import (
    single_row_sql,
    execute_query_iter
)

#==============================================================================
class DBSamplingCrashSource(RequiredConfig):
    """this class will take a random sample of crashes in the jobs table
    and then pull them from whatever primary storages is in use. """

    required_config = Namespace()
    required_config.namespace('main_source')
    required_config.main_source.add_option(
        'crash_storage_class',
        default='socorro.external.hb.crashstorage.HBaseCrashStorage',
        doc='a class for a source of crashes',
        from_string_converter=class_converter
    )
    required_config.namespace('supplemental_source')
    required_config.supplemental_source.add_option(
        'crash_storage_class',
        default='socorro.external.postgresql.crashstorage.' \
                'PostgreSQLCrashStorage',
        doc='a 2nd class for a source of crashes',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'sql',
        default='select uuid from jobs order by queueddatetime DESC '
                'limit 1000',
        doc='an sql string that selects crash_ids',
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        self.config = config
        self.quit_check = quit_check_callback

        self._main_source = config.main_source.crash_storage_class(
            config.main_source,
            quit_check_callback
        )
        self._supplemental_source = \
            config.supplemental_source.crash_storage_class(
                config.supplemental_source,
                quit_check_callback
            )
        self._database = self.config.supplemental_source.database_class(
            self.config.supplemental_source
        )

    #--------------------------------------------------------------------------
    def new_crashes(self):
        self.config.logger.debug('starting new_crashes')
        with self._database() as conn:
            self.quit_check()
            yield_did_not_happen = True
            for a_crash_id in execute_query_iter(conn, self.config.sql):
                self.quit_check()
                yield a_crash_id[0]
                yield_did_not_happen = False
            if yield_did_not_happen:
                yield None

    #--------------------------------------------------------------------------
    def get_raw_crash(self, crash_id):
        """forward the request to the underlying implementation"""
        return self._main_source.get_raw_crash(crash_id)

    #--------------------------------------------------------------------------
    def get_processed_crash(self, crash_id):
        """forward the request to the underlying implementation"""
        processed_crash = self._main_source.get_unredacted_processed(crash_id)
        if 'email' not in processed_crash:
            with self._database() as connection:
                sql = "select email, url, exploitability from reports_20%s " \
                      "where uuid = %%s" % crash_id[-6:]
                email, url, exploitability = single_row_sql(
                    connection,
                    sql,
                    (crash_id, ),
                )
                processed_crash['email'] = email
                processed_crash['url'] = url
                processed_crash['exploitability'] = exploitability
        return processed_crash

#==============================================================================
class PrintingDestination(RequiredConfig):
    required_config = Namespace()

    def __init__(self, config, quit_check_callback=None):
        self.config = config
        self.quit_check_callback = quit_check_callback

    def save_raw_and_processed_crash(self, raw, processed, crash_id):
        if 'email' not in processed:
            print 'email missing from', crash_id
        if 'url' not in processed:
            print 'url missing from', crash_id
        if 'exploitability' not in processed:
            print 'exploitability missing from', crash_id
        print 'not really saving:', crash_id



#==============================================================================
class ElasticMoverApp(FetchTransformSaveApp):
    app_name = 'elastic_mover'
    app_version = '1.0'
    app_description = __doc__

    required_config = Namespace()
    #--------------------------------------------------------------------------
    def transform(self, crash_id):
        """this default transform function only transfers raw data from the
        source to the destination without changing the data.  While this may
        be good enough for the raw crashmover, the processor would override
        this method to create and save processed crashes"""
        try:
            raw_crash = self.source.get_raw_crash(crash_id)
        except Exception as x:
            self.config.logger.error(
                "reading raw_crash: %s",
                str(x),
                exc_info=True
            )
            raw_crash = {}
        try:
            processed_crash = self.source.get_processed_crash(crash_id)
        except Exception as x:
            self.config.logger.error(
                "reading raw_crash: %s",
                str(x),
                exc_info=True
            )
            raw_crash = {}

        try:
            self.destination.save_raw_and_processed_crash(
                raw_crash,
                processed_crash,
                crash_id
            )
        except Exception as x:
            self.config.logger.error(
                "writing raw and processed: %s",
                str(x),
                exc_info=True
            )

    #--------------------------------------------------------------------------
    def source_iterator(self):
        """this iterator yields pathname pairs for raw crashes and raw dumps"""
        for x in self.source.new_crashes():
            if x is None:
                break
            yield ((x,), {})
        self.config.logger.info(
            'the queuing iterator is exhausted - waiting to quit'
        )
        self.task_manager.wait_for_empty_queue(
            5,
            "waiting for the queue to drain before quitting"
        )
        time.sleep(self.config.producer_consumer.number_of_threads * 2)



if __name__ == '__main__':
    main(ElasticMoverApp)
