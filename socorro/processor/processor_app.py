#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""the processor_app converts raw_crashes into processed_crashes"""

import os

from configman import Namespace
from configman.converters import class_converter

from socorro.app.fetch_transform_save_app import FetchTransformSaveApp, main
from socorro.external.filesystem.crashstorage import FileSystemRawCrashStorage
from socorro.external.crashstorage_base import (
  PolyCrashStorage,
  CrashIDNotFound,
)


#==============================================================================
class ProcessorApp(FetchTransformSaveApp):
    """the Socorro processor converts raw_crashes into processed_crashes"""
    app_name = 'processor'
    app_version = '3.0'
    app_description = __doc__

    required_config = Namespace()
    # configuration is broken into three namespaces: processor,
    # new_crash_source, and registrar
    #--------------------------------------------------------------------------
    # processor namespace
    #     this namespace is for config parameter having to do with the
    #     implementation of the algorithm of converting raw crashes into
    #     processed crashes.  This algorithm can be swapped out for alternate
    #     algorithms.
    #--------------------------------------------------------------------------
    required_config.namespace('processor')
    required_config.processor.add_option(
      'processor_class',
      doc='the class that transforms raw crashes into processed crashes',
      default='socorro.processor.hybrid_processor.HybridCrashProcessor',
      from_string_converter=class_converter
    )
    #--------------------------------------------------------------------------
    # new_crash_source namespace
    #     this namespace is for config parameter having to do with the source
    #     of new crash_ids.
    #--------------------------------------------------------------------------
    required_config.namespace('new_crash_source')
    required_config.new_crash_source.add_option(
      'new_crash_source_class',
      doc='an iterable that will stream crash_ids needing processing',
      default='socorro.external.rabbitmq.rmq_new_crash_source'
              '.RMQNewCrashSource',
      from_string_converter=class_converter
    )
    #--------------------------------------------------------------------------
    # registrar namespace
    #     this namespace is for config parameters having to do with registering
    #     the processor so that the monitor is aware of it.
    #--------------------------------------------------------------------------
    required_config.namespace('companion_process')
    required_config.companion_process.add_option(
      'companion_class',
      doc='a classname that runs a process in parallel with the processor',
      default='',      
	  #default='socorro.processor.symbol_cache_manager.SymbolLRUCacheManager',
      from_string_converter=class_converter
    )

    ###########################################################################
    ### TODO: implement an __init__ and a waiting func.  The waiting func
    ### will take registrations of periodic things to do over some time
    ### interval.  the first periodic thing is the rereading of the
    ### signature generation stuff from the database.
    ###########################################################################

    #--------------------------------------------------------------------------
    @staticmethod
    def get_application_defaults():
        return {
            "source.crashstorage_class": FileSystemRawCrashStorage,
            "destination.crashstorage_class": PolyCrashStorage,
        }

    #--------------------------------------------------------------------------
    def source_iterator(self):
        """this iterator yields individual crash_ids from the source
        crashstorage class's 'new_crash_ids' method."""
        self.iterator = self.config.new_crash_source.new_crash_source_class(
          self.config.new_crash_source,
          self.processor_name,
          self.quit_check
        )
        while True:  # loop forever and never raise StopIteration
            for x in self.iterator():
                yield x  # (args, kwargs) or None
            else:
                yield None  # if the inner iterator yielded nothing at all,
                            # yield None to give the caller the chance to sleep

    #--------------------------------------------------------------------------
    def quit_check(self):
        """the quit polling function.  This method, used as a callback, will
        propagate to any thread that loops."""
        self.task_manager.quit_check()

    #--------------------------------------------------------------------------
    def transform(
        self,
        crash_id,
        finished_func=(lambda: None),
    ):
        """this implementation is the framework on how a raw crash is
        converted into a processed crash.  The 'crash_id' passed in is used as
        a key to fetch the raw crash from the 'source', the conversion funtion
        implemented by the 'processor_class' is applied, the
        processed crash is saved to the 'destination', and then 'finished_func'
        is called."""
        try:
            try:
                raw_crash = self.source.get_raw_crash(crash_id)
                dumps = self.source.get_raw_dumps_as_files(crash_id)
            except CrashIDNotFound:
                self.processor.reject_raw_crash(
                    crash_id,
                    'this crash cannot be found in raw crash storage'
                )
                return
            except Exception, x:
                self.config.logger.warning(
                    'error loading crash %s',
                    crash_id,
                    exc_info=True
                )
                self.processor.reject_raw_crash(
                    crash_id,
                    'error in loading: %s' % x
                )
                return

            try:

                if 'uuid' not in raw_crash:
                    raw_crash.uuid = crash_id
                processed_crash = (
                    self.processor.convert_raw_crash_to_processed_crash(
                        raw_crash,
                        dumps
                    )
                )
                """ bug 866973 - save_raw_and_processed() instead of just
                    save_processed().  The raw crash may have been modified
                    by the processor rules.  The individual crash storage
                    implementations may choose to honor re-saving the raw_crash
                    or not.
                """
                self.destination.save_raw_and_processed(
                    raw_crash,
                    None,
                    processed_crash,
                    crash_id
                )
                self.config.logger.info('saved - %s', crash_id)
            finally:
                # earlier, we created the dumps as files on the file system,
                # we need to clean up after ourselves.
                for a_dump_pathname in dumps.itervalues():
                    try:
                        if "TEMPORARY" in a_dump_pathname:
                            os.unlink(a_dump_pathname)
                    except OSError, x:
                        # the file does not actually exist
                        self.config.logger.info(
                            'deletion of dump failed: %s',
                            x,
                        )
        finally:
            # no matter what causes this method to end, we need to make sure
            # that the finished_func gets called. If the new crash source is
            # RabbitMQ, this is what removes the job from the queue.
            try:
                finished_func()
            except Exception, x:
                # when run in a thread, a failure here is not a problem, but if
                # we're running all in the same thread, a failure here could
                # derail the the whole processor. Best just log the problem
                # so that we can continue.
                self.config.logger.error(
                    'Error completing job %s: %s',
                    crash_id,
                    x,
                    exc_info=True
                )

    #--------------------------------------------------------------------------
    def _setup_source_and_destination(self):
        """this method simply instatiates the source, destination,
        new_crash_source, and the processor algorithm implementation."""
        super(ProcessorApp, self)._setup_source_and_destination()
        if self.config.companion_process.companion_class:
            self.companion_process = \
                self.config.companion_process.companion_class(
                    self.config.companion_process,
                    self.quit_check
                )
        else:
            self.companion_process = None

        self.processor_name = "%s.%d:2015" % (
            os.uname()[1].replace('.', '_'),
            os.getpid()
        )
        self.config.processor_name = self.processor_name

        # this function will be called by the MainThread periodically
        # while the threaded_task_manager processes crashes.
        self.waiting_func = None

        self.processor = self.config.processor.processor_class(
          self.config.processor,
          self.quit_check
        )

    #--------------------------------------------------------------------------
    def _cleanup(self):
        """when  the processor shutsdown, this function cleans up"""
        if self.companion_process:
            self.companion_process.close()
        self.iterator.close()


if __name__ == '__main__':
    main(ProcessorApp)
