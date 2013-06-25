#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from configman import Namespace, RequiredConfig
from configman.converters import class_converter
from socorro.external.crashstorage_base import CrashStorageBase


#==============================================================================
class BreakpadPOSTDestination(CrashStorageBase):
    """this a crashstorage derivative that just pushes a crash out to a
    Socorro collector waiting at a url"""
    required_config = Namespace()
    required_config.add_option(
        'url',
        short_form='u',
        doc="The url of the Socorro collector to submit to",
        default="http://127.0.0.1:8882/submit"
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(BreakpadPOSTDestination, self).__init__(
            config,
            quit_check_callback
        )
        self.hang_id_cache = dict()

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dumps, crash_id):
        try:
            for dump_name, dump_pathname in dumps.iteritems():
                if not dump_name:
                    dump_name = self.config.source.dump_field
                raw_crash[dump_name] = open(dump_pathname, 'rb')
            datagen, headers = poster.encode.multipart_encode(raw_crash)
            request = urllib2.Request(
                self.config.url,
                datagen,
                headers
            )
            submission_response = urllib2.urlopen(request).read().strip()
            try:
                self.config.logger.debug(
                    'submitted %s (original crash_id)',
                    raw_crash['uuid']
                )
            except KeyError:
                pass
            self.config.logger.debug(
                'submission response: %s',
                submission_response
                )
            print submission_response
        finally:
            for dump_name, dump_pathname in dumps.iteritems():
                if "TEMPORARY" in dump_pathname:
                    os.unlink(dump_pathname)


#==============================================================================
class BreakpadDBSamplingCrashSource(RequiredConfig):
    """this class will take a random sample of crashes in the jobs table
    and then pull them from whatever primary storages is in use. """

    required_config = Namespace()
    required_config.add_option(
        'source_implementation',
        default='socorro.external.hbase.crashstorage.HBaseCrashStorage',
        doc='a class for a source of raw crashes',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'database_class',
        default='socorro.external.postgresql.connection_context'
                '.ConnectionContext',
        doc='the class that connects to the database',
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
        self._implementation = config.source_implementation(
            config,
            quit_check_callback
        )
        self.config = config
        self.quit_check = quit_check_callback

    #--------------------------------------------------------------------------
    def new_crashes(self):
        self.config.logger.debug('starting new_crashes')
        with self.config.database_class(self.config)() as conn:
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
        return self._implementation.get_raw_crash(crash_id)

    #--------------------------------------------------------------------------
    def get_raw_dumps_as_files(self, crash_id):
        """forward the request to the underlying implementation"""
        return self._implementation.get_raw_dumps_as_files(crash_id)

