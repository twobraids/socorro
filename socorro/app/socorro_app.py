#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import sys
import re


from configman import ConfigurationManager
from configman.converters import class_converter
from configman import Namespace

from socorro.external.filesystem.filesystem import findFileGenerator


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

def find_all_apps():
    def is_socorro_app(candidate):
        return (candidate[1].endswith('_app.py')
                and 'test' not in candidate[1])
    apps_list = []
    for a_path in sys.path:
        try:
            for path, name, pathname in findFileGenerator(
                a_path,
                acceptanceFunction=is_socorro_app):
                    apps_list.append(pathname)
        except OSError:
            pass
    return apps_list


required_config = Namespace()
required_config.add_option(
    'help',
    default=False)

manager = ConfigurationManager(
    [],
    use_auto_help=False,
    use_admin_controls=False,
    ignore_mismatch=True,
)

try:
    application = manager.args[0]
except IndexError:
    import sys
    app_name_re = re.compile(r'.*(socorro.*)/(.*?)_app\.py')
    print >>sys.stderr, '''usage: socorro_app app_name
    where 'app_name' is one of:'''
    for app in sorted(find_all_apps()):
        if '/app/' not in app:
            try:
                app_name = app_name_re.match(app).group(2)
            except AttributeError:
                # no app file
                pass
            else:
                print >>sys.stderr, '       ', app_name
    sys.exit(-1)


full_name_re = re.compile(r'.*(socorro.*)\.py')
m = re.match(full_name_re, find_the_app(application))
module_as_str = m.group(1).replace('/', '.')
app_class_str = "%s%sApp" % (application[0].upper(),
                             application[1:])
full_class_name_str = '.'.join((module_as_str, app_class_str))
app_class = class_converter(full_class_name_str)

from generic_app import main
main(app_class, config_path='.')






