#! /usr/bin/env python
"""the collector recieves crashes from the field"""

# This app can be invoked like this:
#     .../socorro/collector/collector_app.py --help
# set your path to make that simpler
# set both socorro and configman in your PYTHONPATH

import datetime

from socorro.app.generic_app import App, main
from socorro.collector.wsgi_collector import Collector


from configman import Namespace
from configman.converters import class_converter

application = None


#==============================================================================
class CollectorApp(App):
    app_name = 'collector'
    app_version = '4.0'
    app_description = __doc__

    #--------------------------------------------------------------------------
    # in this section, define any configuration requirements
    required_config = Namespace()

    #--------------------------------------------------------------------------
    # collector namespace
    #     the namespace is for config parameters about how to interpret
    #     crash submissions
    #--------------------------------------------------------------------------
    required_config.namespace('collector')
    required_config.collector.add_option(
        'dump_field',
        doc='the name of the form field containing the raw dump',
        default='upload_file_minidump'
    )
    required_config.collector.add_option(
        'dump_id_prefix',
        doc='the prefix to return to the client in front of the OOID',
        default='bp-'
    )

    #--------------------------------------------------------------------------
    # throttler namespace
    #     the namespace is for config parameters for the throttler system
    #--------------------------------------------------------------------------
    required_config.namespace('throttler')
    required_config.throttler.add_option(
        'throttler_class',
        default='socorro.collector.throttler.LegacyThrottler',
        doc='the class that implements the throttling action',
        from_string_converter=class_converter
    )
    #required_config.throttler.add_aggregation(
        #'throttler',
        #lambda c, lc, a: lc.throttler_class(lc)  # instantiate the throttler
    #)

    #--------------------------------------------------------------------------
    # storage namespace
    #     the namespace is for config parameters crash storage
    #--------------------------------------------------------------------------
    required_config.namespace('storage')
    required_config.storage.add_option(
        'crashstorage_class',
        doc='the source storage class',
        default='socorro.external.filesystem.crashstorage.'
        'FileSystemRawCrashStorage',
        from_string_converter=class_converter
    )
    #required_config.storage.add_aggregation(
        #'crash_storage',
        #lambda c, lc, a: lc.crashstorage_class(lc)  # instantiate crash_storage
    #)

    #--------------------------------------------------------------------------
    # web_server namespace
    #     the namespace is for config parameters the web server
    #--------------------------------------------------------------------------
    required_config.namespace('web_server')
    required_config.web_server.add_option(
        'web_server_class',
        doc='a class implementing a web server',
        default='socorro.webapi.servers.CherryPy',
        from_string_converter=class_converter
    )

    #--------------------------------------------------------------------------
    def main(self):
        global application

        services_list = (
            Collector,
        )
        self.config.crash_storage = self.config.storage.crashstorage_class(
            self.config.storage
        )
        self.config.throttler = self.config.throttler.throttler_class(
            self.config.throttler
        )
        self.web_server = self.config.web_server.web_server_class(
            self.config,  # needs the whole config rather than just the namespace
            services_list
        )

        application = self.web_server.run()


if __name__ == '__main__':
    main(CollectorApp)