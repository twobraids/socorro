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

from socorro.app.fetch_transform_save_app import FetchTransformSaveApp


#==============================================================================
class CrashMoverApp(FetchTransformSaveApp):
    """move new crashes from one storage system to another"""
    app_name = 'storage_mover'
    app_version = '2.0'
    app_description = __doc__

    #--------------------------------------------------------------------------
    def transform(self, ooid):
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
        self.source.delete(ooid)


if __name__ == '__main__':
    main(StorageMoverApp)


