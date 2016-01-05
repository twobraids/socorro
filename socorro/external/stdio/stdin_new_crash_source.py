# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from configman import Namespace, RequiredConfig
from configman.converters import class_converter

from sys import stdin


#==============================================================================
class StdinNewCrashSource(RequiredConfig):
    """An iterable of crashes from RabbitMQ"""


    #--------------------------------------------------------------------------
    def __init__(self, config, name=None, quit_check_callback=None):
        self.config = config

    #--------------------------------------------------------------------------
    def close(self):
        pass

    #--------------------------------------------------------------------------
    def __iter__(self):
        for a_crash_id in stdin:
            yield (
                (a_crash_id.strip(),),
                {}
            )

    #--------------------------------------------------------------------------
    def new_crashes(self):
        return self.__iter__()

    #--------------------------------------------------------------------------
    def __call__(self):
        return self.__iter__()
