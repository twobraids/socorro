# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import json

from socorro.external.crashstorage_base import (
    CrashStorageBase,
    CrashIDNotFound
)
from configman import (
    Namespace,
    class_converter
)
from socorro.external.postgresql.connection_context import ConnectionContext
from socorro.lib.datetimeutil import uuid_to_date, JsonDTEncoder
from socorro.external.postgresql.dbapi2_util import (
    SQLDidNotReturnSingleValue,
    single_value_sql,
    execute_no_results
)


#==============================================================================
class StdioCrashStorage(CrashStorageBase):
    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(StdioCrashStorage, self).__init__(
            config,
            quit_check_callback=quit_check_callback
        )

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dumps, crash_id):
        """nota bene: this function does not save the dumps in PG, only
        the raw crash json is saved."""
        print raw_crash.dump_checksums['upload_file_minidump']


    #--------------------------------------------------------------------------
    def new_crashes(self):
        for crash_id in raw_input():
            yield crash_id
