# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import web
import logging

logger = logging.getLogger("collector")

import socorro.lib.ooid as sooid
import socorro.storage.crashstorage as cstore

from socorro.lib.datetimeutil import utc_now
from socorro.lib.util import DotDict

#===============================================================================
class Collector(object):
  #-----------------------------------------------------------------------------
  def __init__(self, context):
    self.context = context
    self.logger = self.context.setdefault('logger', logger)
    #self.logger.debug('Collector __init__')
    self.legacyThrottler = context.legacyThrottler # save 1 level of lookup later
    self.dumpIDPrefix = context.dumpIDPrefix # save 1 level of lookup later

  #-----------------------------------------------------------------------------
  uri = '/submit'
  #-----------------------------------------------------------------------------
  def POST(self, *args):
    crashStorage = self.context.crashStoragePool.crashStorage()
    the_form = web.input()

    # get the dumps out of the form
    dumps = DotDict()
    for (key, value) in web.webapi.rawinput().iteritems():
      if hasattr(value, 'file') and hasattr(value, 'value'):
        if key == self.context.dumpField:
          # to maintain backwards compatibility the main dump must
          # have the name 'dump'
          dumps['dump'] = the_form.value
        else:
          dumps[key] = the_form.value
        del the_form[key]

    currentTimestamp = utc_now()
    raw_crash = crashStorage.makeJsonDictFromForm(the_form)
    raw_crash.dump_names = dumps.keys()
    raw_crash.submitted_timestamp = currentTimestamp.isoformat()
    #for future use when we start sunsetting products
    #if crashStorage.terminated(raw_crash):
      #return "Terminated=%s" % raw_crash.Version
    crash_id = sooid.createNewOoid(currentTimestamp)
    raw_crash.legacy_processing = \
        self.legacyThrottler.throttle(raw_crash)

    if raw_crash.legacy_processing == cstore.LegacyThrottler.IGNORE:
      self.logger.info('%s ignored', crash_id)
      return "Unsupported=1\n"

    self.logger.info('%s received', crash_id)
    result = crashStorage.save_raw(crash_id,
                                   raw_crash,
                                   dumps,
                                   currentTimestamp)
    if result == cstore.CrashStorageSystem.DISCARDED:
      return "Discarded=1\n"
    elif result == cstore.CrashStorageSystem.ERROR:
      raise Exception("CrashStorageSystem ERROR")
    return "CrashID=%s%s\n" % (self.dumpIDPrefix, crash_id)
