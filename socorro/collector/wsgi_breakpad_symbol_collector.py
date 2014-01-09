# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import web
import time

from socorro.lib.ooid import createNewOoid
from socorro.lib.util import DotDict
from socorro.lib.datetimeutil import utc_now

from configman import RequiredConfig, Namespace


#==============================================================================
class BreakpadSymbolCollector(RequiredConfig):
    #--------------------------------------------------------------------------
    # in this section, define any configuration requirements
    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(self, config):
        self.config = config
        self.logger = self.config.logger

    #--------------------------------------------------------------------------
    uri = '/submit'
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _make_raw_symbol_meta_and_binary(self, form):
        binary_symbols = DotDict()
        symbol_meta = DotDict()
        for name, value in form.iteritems():
            if isinstance(value, basestring):
                symbol_meta[name] = value
            elif hasattr(value, 'file') and hasattr(value, 'value'):
                binary_symbols[name] = value.value
            else:
                symbol_meta[name] = value.value
        return symbol_meta, binary_symbols

    #--------------------------------------------------------------------------
    def POST(self, *args):
        symbol_meta, binary_symbols = \
            self._make_symbol_meta_and_binary_symbols(web.webapi.rawinput())

        current_timestamp = utc_now()
        symbol_meta.submitted_timestamp = current_timestamp.isoformat()

        symbol_id = createNewOoid(current_timestamp)
        symbol_meta.uuid = symbol_id
        self.logger.info('%s received', symbol_id)

        self.config.symbol_storage.save_symbol_meta(
          symbol_meta,
          binary_symbols,
          symbol_id
        )
        self.logger.info('%s accepted', symbol_id)
        return "symbol_id=%s\n" % symbol_id
