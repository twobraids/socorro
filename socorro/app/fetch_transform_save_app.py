"""this the is basis for any app that follows the fetch/transform/save model"""

import types

from configman import Namespace
from configman.converters import class_converter
from socorro.lib.threaded_task_manager import ThreadedTaskManager
from socorro.app.generic_app import App


class FetchTransformSaveApp(App):
    app_name = 'generic_fetch_transform_save_app'
    app_version = '0.1'
    app_description = __doc__

    required_config = Namespace()
    required_config.source = Namespace()
    required_config.source.add_option('crashstorage',
                                      doc='the source storage class',
                                      default=None,
                                      from_string_converter=class_converter)
    required_config.destination = Namespace()
    required_config.destination.add_option('crashstorage',
                                        doc='the destination storage class',
                                        default=None,
                                        from_string_converter=class_converter)

    def source_iterator(self):  # never raises StopIteration
        while(True):
            for x in self.source.new_ooids():
                yield ((x,), {})

    def transform(self, ooid):
        """this default transform function only transfers from the source
        to the destination without changing the data"""
        raw_crash = self.source.get_raw_crash(ooid)
        dump = self.source.get_dump(ooid)
        self.destination.save_raw_crash(raw_crash, dump)

    def main(self):
        try:
            self.source = self.config.source.crashstorage(self.config)
        except TypeError:  # the None case
            self.source = None
        try:
            self.destination = self.config.destination.crashstorage(
              self.config)
        except TypeError:  # the None case
            self.destination = None

        self.task_manager = ThreadedTaskManager(
          self.config,
          job_source_iterator=self.source_iterator,
          task_func=self.transform)

        self.task_manager.blocking_start()


