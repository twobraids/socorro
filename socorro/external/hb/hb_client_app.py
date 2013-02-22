#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""demonstrates using configman to make a Socorro app"""

# This app can be invoked like this:
#     .../socorro/app/example_app.py --help
# set your path to make that simpler
# set both socorro and configman in your PYTHONPATH

import datetime

from socorro.app.generic_app import App, main

from configman import Namespace, RequiredConfig
from configman.converters import class_converter

class CommandWithCrashID(RequiredConfig):
    required_config = Namespace()
    required_config.add_aggregation(
        'crash_id',
        lambda g, l, a: a[1]
    )


class get_raw_crash(CommandWithCrashID):
    """get the raw crash json data"""
    def run(self):
        storage = self.config.storage
        crash_id = self.config.crash_id
        pprint.pprint(storage.get_raw_crash(crash_id))


class get_raw_dumps(CommandWithCrashID):
    """get information on the raw dumps for a crash"""
    def run(self):
        storage = self.config.storage
        crash_id = self.config.crash_id
        for name, dump in storage.get_raw_dumps(crash_id).items():
            print("%s: dump length = %s" % (name, len(dump)))


class get_processed(CommandWithCrashIDv):
    """get the processed json for a crash"""
    def run(self):
        storage = self.config.storage
        crash_id = self.config.crash_id
        pprint.pprint(storage.get_processed(crash_id))


class get_report_processing_state(CommandWithCrashID):
    """get the report processing state for a crash"""
    def run(self):
        @storage._run_in_transaction
        def transaction(conn):
            pprint.pprint(storage._get_report_processing_state(conn, crash_id))
        transaction()


def get_limit(globals, locals, args):
    try:
        return args[4]
    except IndexError:
        return 10


class CommandsRequiringTable(RequiredConfig):
    required_config = Namespace()
    required_config.add_aggregation(
        'table',
        lambda g, l, a: a[1]
    )


class CommandsRequiringTablePrefixColumnsLimit(RequiredConfig):
    required_config = Namespace()
    required_config.add_aggregation(
        'prefix',
        lambda g, l, a: a[2]
    )
    required_config.add_aggregation(
        'columns',
        lambda g, l, a: a[3]
    )
    required_config.add_aggregation(
        'limit',
        get_limit
    )


class union_scan_with_prefix(CommandsRequiringTablePrefixColumnsLimit):
    """do a union scan on a table using a given prefix"""
    def run(self):
        @storage._run_in_transaction
        def transaction(conn, limit=limit):
            if limit is None:
                limit = 10
            for row in itertools.islice(
                            storage._union_scan_with_prefix(
                                conn,
                                table,
                                prefix,
                                columns),
                            limit
                        ):
                pprint.pprint(row)


class merge_scan_with_prefix(CommandsRequiringTablePrefixColumnsLimit):
    """do a merge scan on a table using a given prefix"""
    def run(self):
        @storage._run_in_transaction
        def transaction(conn, limit=limit):
            if limit is None:
                limit = 10
            for row in itertools.islice(
                           storage._merge_scan_with_prefix(
                               conn,
                               table,
                               prefix,
                               columns),
                           limit):
                pprint.pprint(row)


class describe_table(CommandsRequiringTable):
    def run(self):
        @storage._run_in_transaction
        def transaction(conn):
            pprint.pprint(conn.getColumnDescriptors(table))


class get_full_row(CommandsRequiringTable):
    def run(self):
        @storage._run_in_transaction
        def transaction(conn):
            pprint.pprint(storage._make_row_nice(conn.getRow(table, row_id)[0]))


#==============================================================================
class HBaseClientApp(App):
    app_name = 'hbase_client'
    app_version = '2.0'
    app_description = __doc__

    #--------------------------------------------------------------------------
    # in this section, define any configuration requirements
    required_config = Namespace()
    required_config.add_aggregation(
        'command',
        function=lambda g, l, a: class_converter(a[1])
    )
    required_config.add_option(
        'hbase_crash_storage_class',
        default=HBaseCrashStorage,
        doc='the class responsible for providing an hbase connection',
        from_string_converter=class_converter
    )


    #--------------------------------------------------------------------------
    def main(self):
        self.config.command.run()


if __name__ == '__main__':
    main(HBaseClientApp)
