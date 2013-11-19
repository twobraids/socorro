#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""a general executor for socorro applications"""

import datetime
import sys
import re

from socorro.app.generic_app import App, main

from configman import Namespace, class_converter
from socorro.external.filesystem.filesystem import findFileGenerator


#------------------------------------------------------------------------------
def find_the_app(app_name):
    def is_socorro_app(candidate):
        return candidate[1].endswith('%s_app.py' % app_name)
    for a_path in sys.path:
        try:
            for path, name, pathname in findFileGenerator(
                a_path,
                acceptanceFunction=is_socorro_app):
                    return pathname
        except OSError:
            pass
    return None


#------------------------------------------------------------------------------
def find_all_apps():
    def is_socorro_app(candidate):
        return (candidate[1].endswith('_app.py')
                and 'test' not in candidate[1]
                and 'socorro' not in candidate[1]
                and 'example' not in candidate[1]
                and 'fetch_transform' not in candidate[1]
                and 'generic' not in candidate[1])
    apps_list = set()
    for a_path in sys.path:
        try:
            for path, name, pathname in findFileGenerator(
                a_path,
                acceptanceFunction=is_socorro_app):
                    apps_list.add(name.replace('_app.py', ''))
        except OSError:
            pass
    return apps_list


#------------------------------------------------------------------------------
def name_to_class_converter(an_app_name):
    if not an_app_name:
        return None
    app_path_name = find_the_app(an_app_name)
    if not app_path_name:
        raise Exception('%s could not be found' % an_app_name)
    full_name_re = re.compile(r'.*(socorro.*)\.py')
    m = re.match(full_name_re, app_path_name)
    module_as_str = m.group(1).replace('/', '.')
    app_class_str = "%s%sApp" % (an_app_name[0].upper(),
                                 an_app_name[1:])
    full_class_name_str = '.'.join((module_as_str, app_class_str))
    #return full_class_name_str
    return class_converter(full_class_name_str)


#==============================================================================
class SocorroApp(App):
    app_name = 'socorro'
    app_version = '66'
    app_description = __doc__

    #--------------------------------------------------------------------------
    # in this section, define any configuration requirements
    required_config = Namespace()
    required_config.add_option(
        'target_application',
        default='',
        doc='the name of the application to run (%s)' % ', '.join(
            find_all_apps()
        ),
        is_argument=True,
        from_string_converter=name_to_class_converter,
    )

    #--------------------------------------------------------------------------
    def main(self):
        if self.config.target_application:
            target_app = self.config.target_application(self.config)
            return target_app.main()
        else:
            return -1


if __name__ == '__main__':
    main(SocorroApp)
