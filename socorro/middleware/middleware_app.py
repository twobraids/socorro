#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""implementation of the Socorro data service"""

# This app can be invoked like this:
#     .../socorro/collector/middleware_app.py --help
# replace the ".../" with something that makes sense for your environment
# set both socorro and configman in your PYTHONPATH

import os

from socorro.app.generic_app import App, main
from socorro.external.package_loader import package_list_converter

import socorro.services
import socorro.middleware

from configman import Namespace
from configman.converters import class_converter

# an app running under modwsgi needs to have a name at the module level called
# application.  The value is set in the App's 'main' function below.  Only the
# modwsgi Apache version actually makes use of this variable.
application = None


#==============================================================================
class MiddlewareApp(App):
    app_name = 'middleware'
    app_version = '3.0'
    app_description = __doc__

    services_list = []

    #--------------------------------------------------------------------------
    # in this section, define any configuration requirements
    required_config = Namespace()

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

    #--------------------------------------------------------------------------
    # implementations namespace
    #     the namespace is for external implementations of the services
    #-------------------------------------------------------------------------
    required_config.namespace('implementions')
    required_config.implementions.add_option(
      'impl_list',
      doc='list of packages for service implementations',
      default='socorro.external.postgresql, socorro.external.elasticsearch',
      from_string_converter=package_list_converter
    )

    #--------------------------------------------------------------------------
    # services namespace
    #     the namespace is for config parameters crash storage
    #--------------------------------------------------------------------------
    required_config.namespace('services')
    required_config.services.add_option('fred')
    print os.listdir(socorro.middleware.__path__[0])
    for a_source_file in os.listdir(socorro.middleware.__path__[0]):
        if a_source_file.endswith('_service.py'):
            print 'doing:', a_source_file
            service_name = a_source_file.replace('_service.py', '')
            required_config.services.namespace(service_name)
            service_class = class_converter(
              'socorro.middleware.%s_service.Service' % service_name
            )
            try:
                required_config.services[service_name] = \
                  service_class.get_required_config()
            except AttributeError:
                pass

    #--------------------------------------------------------------------------
    # web_server namespace
    #     the namespace is for config parameters the web server
    #--------------------------------------------------------------------------
    required_config.namespace('web_server')
    required_config.web_server.add_option(
        'wsgi_server_class',
        doc='a class implementing a wsgi web server',
        default='socorro.webapi.servers.CherryPy',
        from_string_converter=class_converter
    )

    #--------------------------------------------------------------------------
    def main(self):
        # Apache modwsgi requireds a module level name 'applicaiton'
        global application

        services_list = (
            Collector,
        )
        self.config.crash_storage = self.config.storage.crashstorage_class(
            self.config.storage
        )
        self.web_server = self.config.web_server.wsgi_server_class(
            self.config,  # needs the whole config not the local namespace
            services_list
        )

        # for modwsgi the 'run' method returns the wsgi function that Apache
        # will use.  For other webservers, the 'run' method actually starts
        # the standalone web server.
        application = self.web_server.run()


if __name__ == '__main__':
    main(MiddlewareApp)
