# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import contextlib
import psycopg2

from configman import Namespace, class_converter

from .dbapi2_util import (
    execute_query_fetchall,
    execute_no_results,
    single_value_sql,
)
from socorro.external import DatabaseError
from socorro.webapi.webapiService import DataserviceWebServiceBase


#==============================================================================
class PostgreSQLWebServiceBase(DataserviceWebServiceBase):

    """
    Base class for PostgreSQL based service implementations.
    """

    required_config = Namespace()
    required_config.add_option(
        'crashstorage_class',
        doc='the source storage class',
        default='socorro'
            '.external.postgresql.crashstorage.PostgreSQLCrashStorage',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'output_is_json',
        doc='Does this service provide json output?',
        default=True,
    )
    required_config.add_option(
        'cache_seconds',
        doc='number of seconds to store results in filesystem cache',
        default=3600,
    )

    #--------------------------------------------------------------------------
    def __init__(self, config):
        """
        Store the config and create a connection to the database.

        Keyword arguments:
        config -- Configuration of the application.

        """
        super(PostgreSQLWebServiceBase, self).__init__(config)

    #--------------------------------------------------------------------------
    @staticmethod
    def parse_versions(versions_list, products):
        """
        Parses the versions, separating by ":" and returning versions
        and products.
        """
        versions = []

        for v in versions_list:
            if v.find(":") > -1:
                pv = v.split(":")
                versions.append(pv[0])
                versions.append(pv[1])
            else:
                products.append(v)

        return (versions, products)

    #--------------------------------------------------------------------------
    @staticmethod
    def prepare_terms(terms, search_mode):
        """
        Prepare terms for search, adding '%' where needed,
        given the search mode.
        """
        if search_mode in ("contains", "starts_with"):
            terms = terms.replace("_", "\_").replace("%", "\%")

        if search_mode == "contains":
            terms = "%" + terms + "%"
        elif search_mode == "starts_with":
            terms = terms + "%"
        return terms

    #--------------------------------------------------------------------------
    @staticmethod
    def dispatch_params(sql_params, key, value):
        """
        Dispatch a parameter or a list of parameters into the params array.
        """
        if not isinstance(value, list):
            sql_params[key] = value
        else:
            for i, elem in enumerate(value):
                sql_params[key + str(i)] = elem
        return sql_params
