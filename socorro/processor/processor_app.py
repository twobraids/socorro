"""the processor_app converts raw_crashes into processed_crashes"""

import copy

from configman import Namespace
from configman.converters import class_converter

from socorro.app.fetch_transform_save_app import FetchTransformSaveApp, main
from socorro.external.postgresql.crashstorage import PostgreSQLCrashStorage
from socorro.external.crashstorage_base import (
  PolyCrashStorage,
  OOIDNotFoundException,
)


#==============================================================================
class ProcessorApp(FetchTransformSaveApp):
    """the Socorro processor converts raw_crashes into processed_crashes"""
    app_name = 'processor_app'
    app_version = '3.0'
    app_description = __doc__

    # set the Option defaults in the parent class to values that make sense
    # for the context of this app
    FetchTransformSaveApp.required_config.source.crashstorage.set_default(
      PostgreSQLCrashStorage
    )
    FetchTransformSaveApp.required_config.destination.crashstorage.set_default(
      PolyCrashStorage
    )

    required_config = Namespace()
    # configuration is broken into three namespaces: processor, ooid_source,
    # and registrar
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
      default='socorro.processor.legacy_processor.LegacyCrashProcessor',
      from_string_converter=class_converter
    )
    #--------------------------------------------------------------------------
    # ooid_source namespace
    #     this namespace is for config parameter having to do with the source
    #     of new ooids.
    #--------------------------------------------------------------------------
    required_config.namespace('ooid_source')
    required_config.ooid_source.add_option(
      'ooid_source_class',
      doc='an iterable that will stream ooids needing processing',
      default='socorro.processor.legacy_ooid_source.LegacyOoidSource',
      from_string_converter=class_converter
    )
    #--------------------------------------------------------------------------
    # registrar namespace
    #     this namespace is for config parameters having to do with registering
    #     the processor so that the monitor is aware of it.
    #--------------------------------------------------------------------------
    required_config.namespace('registrar')
    required_config.registrar.add_option(
      'registrar_class',
      doc='the class that registers and tracks processors',
      default='socorro.processor.registration_client.'
              'ProcessorAppRegistrationClient',
      from_string_converter=class_converter
    )

    #--------------------------------------------------------------------------
    def source_iterator(self):
        """this iterator yields individual ooids from the source crashstorage
        class's 'new_ooids' method."""
        self.iterator = self.config.ooid_source.ooid_source_class(
          self.config.ooid_source,
          self.registrar.processor_name,
          self.quit_check
        )
        while True:  # loop forever and never raise StopIteration
            for x in self.iterator():
                if x is None:
                    yield None
                else:
                    yield ((x,), {})  # (args, kwargs)
            else:
                yield None  # if the inner iterator yielded nothing at all,
                            # yield None to give the caller the chance to sleep

    #--------------------------------------------------------------------------
    def quit_check(self):
        """the quit polling function.  This method, used as a callback, will
        propagate to any thread that loops."""
        self.task_manager.quit_check()

    #--------------------------------------------------------------------------
    def transform(self, ooid):
        """this implementation is the framework on how a raw crash is
        converted into a processed crash.  The 'ooid' passed in is used as a
        key to fetch the raw crash from the 'source', the conversion funtion
        implemented by the 'processor_class' is applied, and then the
        processed crash is saved to the 'destination'."""
        try:
            raw_crash = self.source.get_raw_crash(ooid)
        except OOIDNotFoundException:
            self.processor.reject_raw_crash(
              ooid,
              'this crash cannot be found in raw crash storage'
            )
            return

        dump = self.source.get_raw_dump(ooid)
        if 'uuid' not in raw_crash:
            raw_crash.uuid = ooid
        processed_crash = \
          self.processor.convert_raw_crash_to_processed_crash(
            raw_crash,
            dump
          )
        self.destination.save_processed(processed_crash)

    #--------------------------------------------------------------------------
    def _setup_source_and_destination(self):
        """this method simply instatiates the source, destination, ooid_source,
        and the processor algorithm implementation."""
        super(ProcessorApp, self)._setup_source_and_destination()
        self.registrar = self.config.registrar.registrar_class(
          self.config.registrar,
          self.quit_check
        )
        # this function will be called by the MainThread periodically
        # while the threaded_task_manager processes crashes.
        self.waiting_func = self.registrar.checkin

        self.processor = self.config.processor.processor_class(
          self.config.processor,
          self.quit_check
        )

    #--------------------------------------------------------------------------
    def _cleanup(self):
        """when  the processor shutsdown, this function cleans up"""
        self.registrar.unregister()
        self.iterator.close()


if __name__ == '__main__':
    main(ProcessorApp)