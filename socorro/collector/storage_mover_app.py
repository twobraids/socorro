#! /usr/bin/env python
"""move new crashes from one storage system to another"""

# This app can be invoked like this:
#     .../socorro/app/example_app.py --help
# set your path to make that simpler
# set both socorro and configman in your PYTHONPATH

import datetime

from socorro.app.generic_app import App, main

from configman import Namespace

try:
    import json
except ImportError:
    import simplejson as json

import signal

import socorro.lib.JsonDumpStorage as jds
import socorro.storage.crashstorage as cstore
import socorro.lib.util as sutil
from socorro.lib.threaded_task_manager import ThreadedTaskManager


#==============================================================================
class StorageMoverApp(App):
    app_name = 'storage_mover'
    app_version = '2.0'
    app_description = __doc__

    #--------------------------------------------------------------------------
    # in this section, define any configuration requirements
    required_config = Namespace()
    required_config.add_option('task_manager_class',
                               default=ThreadedTaskManager,
                               doc='the class that runs tasks')
    required_config.source = Namespace()
    required_config.source.add_option('storage_class',
                                default=cstore.CrashStorageSystemForLocalFS,
                                doc='the class for the source of new crashes')
    required_config.destination = Namespace()
    required_config.destination.add_option('storage_class',
                                default=cstore.CrashStorageSystemForHBase,
                                doc='the class for the destination of new '
                                    'crashes')

    #--------------------------------------------------------------------------
    def __init__(self, config):
        super(StorageMoverApp,self).__init__(config)
        self.source = config.source.storage_class(config)
        self.destination = config.destination.storage_class(config)

    #--------------------------------------------------------------------------
    def the_iterator(self):
        """This infinite iterator will walk through the file system storage,
        yielding the ooids of every new entry in the filelsystem.  If there
        are no new entries, it yields None"""
        try:
            while True:
                i = 0
                for i, ooid in enumerate(self.source.newUuids()):
                    yield (ooid,)
                if i == 0:
                    yield None
        except KeyboardInterrupt:
            self.logger.info()

    #--------------------------------------------------------------------------
    def do_submission(self, ooid):
        try:
            try:
                jsonContents = self.source.get_meta(ooid)
            except ValueError:
                logger.warning('the json for %s is degenerate and cannot '
                               'be loaded - saving empty json', ooid)
                jsonContents = {}
            dumpContents = self.source.get_raw_dump(ooid)
            logger.debug('pushing %s to dest', ooid)
            self.destination.save_raw(ooid,
                                      jsonContents,
                                      dumpContents)
            sourceStorage.quickDelete(ooid)
        except Exception:
            sutil.reportExceptionAndContinue(self.config.logger)

    #--------------------------------------------------------------------------
    def main(self):
        task_manager = self.config.task_manager_class(self.config)
        task_manager.blocking_start()

if __name__ == '__main__':
    main(ExampleApp)


