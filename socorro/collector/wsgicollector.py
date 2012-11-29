# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import web
import time
import logging

logger = logging.getLogger("collector")

from socorro.lib.ooid import createNewOoid
from socorro.lib.datetimeutil import utc_now
from socorro.storage.crashstorage import LegacyThrottler

from socorro.external.crashstorage_base import PolyStorageError

from socorro.lib.util import DotDict


#==============================================================================
class Collector(object):
    #--------------------------------------------------------------------------
    def __init__(self, context):
        self.context = context
        self.logger = self.context.setdefault('logger', logger)
        self.legacy_throttler = context.legacyThrottler
        self.dump_id_prefix = context.dumpIDPrefix
        self.dump_field = context.dumpField

    #--------------------------------------------------------------------------
    uri = '/submit'

    #--------------------------------------------------------------------------
    def make_raw_crash(self, form):
        raw_crash = DotDict()
        for name in form.keys():
            if isinstance(form[name], basestring):
                raw_crash[name] = form[name]
            else:
                raw_crash[name] = form[name].value
        raw_crash.timestamp = time.time()
        return raw_crash

    #--------------------------------------------------------------------------
    def POST(self, *args):
        the_form = web.input()

        # get the dumps out of the form
        dumps = DotDict()
        for (key, value) in web.webapi.rawinput().iteritems():
            if hasattr(value, 'file') and hasattr(value, 'value'):
                if key == self.dump_field:
                    dumps[self.dump_field] = value.value
                else:
                    dumps[key] = value.value
                del the_form[key]

        raw_crash = self.make_raw_crash(the_form)

        current_timestamp = utc_now()
        raw_crash.submitted_timestamp = current_timestamp.isoformat()

        crash_id = createNewOoid(current_timestamp)

        raw_crash.legacy_processing = self.legacy_throttler.throttle(raw_crash)
        if raw_crash.legacy_processing == LegacyThrottler.DISCARD:
            self.logger.info('%s discarded', crash_id)
            return "Discarded=1\n"
        if raw_crash.legacy_processing == LegacyThrottler.IGNORE:
            self.logger.info('%s ignored', crash_id)
            return "Unsupported=1\n"

        crash_storage = self.context.crashStoragePool.crashStorage()
        try:
            crash_storage.save_raw_crash(
                raw_crash,
                dumps,
                crash_id
            )
        except PolyStorageError, x:
            self.logger.error('storage exception: %s', str(x.exceptions))
            raise
        self.logger.info('%s accepted', crash_id)
        return "CrashID=%s%s\n" % (self.dump_id_prefix, crash_id)
