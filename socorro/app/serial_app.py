#! /usr/bin/env python
"""demonstrates using configman to make a sequence Socorro app"""

import datetime

from socorro.app.generic_app import SequenceApp, main

from configman import Namespace


#==============================================================================
class SerialApp(SequenceApp):
    app_name = 'serial'
    app_version = '0.1'
    app_description = __doc__


if __name__ == '__main__':
    main(SerialApp)
