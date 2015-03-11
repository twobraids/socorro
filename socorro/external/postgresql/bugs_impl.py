# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from configman import Namespace, RequiredConfig, class_converter
from socorro.external.postgresql.dbapi2_util import execute_query_fetchall

#==============================================================================
class Bugs(RequiredConfig):
    """Implement the /bugs service with PostgreSQL. """

    required_config = Namespace()
    required_config.add_option(
        'crashstorage_class',
        doc='the source storage class',
        default='socorro'
            '.external.postgresql.crashstorage.PostgreSQLCrashStorage',
        from_string_converter=class_converter
    )

    def __init__(self, config):
        self.config = config
        crashstore = config.crashstorage_class(config)
        self.transaction = crashstore.transaction

    #--------------------------------------------------------------------------
    def action(self, action_name, additional_parameters):
        function = getattr(self, action_name)
        results = function(additional_parameters)
        bugs = []
        for row in results:
            bug = dict(zip(("signature", "id"), row))
            bugs.append(bug)
        return bugs

    #--------------------------------------------------------------------------
    def signatures(self, bug_id):
        sql = """/* socorro.external.postgresql.bugs.Bugs.get */
            SELECT ba.signature, bugs.id
            FROM bugs
                JOIN bug_associations AS ba ON bugs.id = ba.bug_id
            WHERE bugs.id IN %s
        """
        return self.transaction(
            execute_query_fetchall,
            sql,
            (bug_id, ),
        )

    #--------------------------------------------------------------------------
    def bug_ids(self, signature):
        sql = """/* socorro.external.postgresql.bugs.Bugs.get */
            SELECT ba.signature, bugs.id
            FROM bugs
                JOIN bug_associations AS ba ON bugs.id = ba.bug_id
            WHERE EXISTS(
                SELECT 1 FROM bug_associations
                WHERE bug_associations.bug_id = bugs.id
                AND signature IN %s
            )
        """
        return self.transaction(
            execute_query_fetchall,
            sql,
            (signature, ),
        )
