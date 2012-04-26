"""the processor that transforms raw crashes into processed crashes"""

from socorro.app.fetch_transform_save_app import FetchTransformSaveApp

from socorro.external.hbase.crashstorage import HBaseCrashStorage
from socorro.external.crashstorage_base import PolyCrashStorage

from configman import Namespace
from configman.converters import class_converter


#==============================================================================
class ProcessorApp(FetchTransformSaveApp):
    app_name = 'processor_app'
    app_version = '2.0'
    app_description = __doc__

    required_config = Namespace()
    required_config.add_option(
      'registration_agent',
      default='socorro.processor.registration.ProcessorAppRegistrationAgent',
      doc='the class that will register the '
          'the processor with the controlling '
          'authority',
      from_string_converter=class_converter
    )
    required_config.namespace('transform')
    required_config.transform.add_option(
      'crash_processor_class',
      default='socorro.processor.legacy.LegacyProcessor',
      doc='the class that will convert raw crashes into processed crashes',
      from_string_converter=class_converter
    )
    required_config.namespace('iterator')
    required_config.ooid_source.add_option(
      'ooid_source_class',
      default='socorro.processor.legacy.LegacyOoidSource',
      doc='the class that will produce a stream of ooids for processing',
      from_string_converter=class_converter
    )
    # override the default on the parent class source crash storage
    FetchTransformSaveApp.required_config.source.crashstorage.default = \
        HBaseCrashStorage
    # override the default on the parent class destination crash storage
    FetchTransformSaveApp.required_config.source.crashstorage.default = \
        PolyCrashStorage

    #--------------------------------------------------------------------------
    def _setup_registration(self):
        self.registrar = self.config.registration_agent(
          config,
          self.quit_check
        )
        self.processor_name = self.registrar.processor_name

    #--------------------------------------------------------------------------
    def _setup_ooid_source(self):
        self.ooid_source = self.config.iterator.ooid_source_class(
          config.iterator,
          self.processor_name,
          self.quit_check
        )

    #--------------------------------------------------------------------------
    def _setup_transform(self):
        self.transform_raw_crash_to_processed_crash = \
          self.config.transform.crash_processor_class(
            config,
            self.processor_name,
            self.quit_check
          )

    #--------------------------------------------------------------------------
    def transform(self, ooid):
        raw_crash = self.source.get_raw_crash(ooid)
        raw_dump = self.source.get_raw_dump(ooid)
        processed_crash = self.transform_raw_crash_to_processed_crash(
          raw_crash,
          raw_dump
        )
        self.destination.save_processed(processed_crash)

    #--------------------------------------------------------------------------
    def main(self):
        self.setup_source_and_destination()
        self._setup_registration()
        self._setup_ooid_source()
        try:
            self._setup_transform()
            self.setup_task_manager()
            self.task_manager.blocking_start()
        finally:
            self.ooid_source.close()




