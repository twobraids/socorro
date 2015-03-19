# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from configman import Namespace

from socorro.external import MissingArgumentError, BadArgumentError
from socorro.dataservice.util import (
    ServiceBase
)
from socorro.lib.converters import change_default
from socorro.lib import external_common


#==============================================================================
class BugsService(ServiceBase):
    """this is a wrapper class that gives a Web Sevice API to a implementation
    of the bugs service.  The implementation is loaded by configman and can be
    any class that implements 'signature' and 'bug_ids' methods.  Currently,
    the only implementation is in PostgreSQL, but having the separation of
    interface from implementation allows other users in the future to easily
    replace it."""

    required_config = Namespace()
    required_config.uri = change_default(
        ServiceBase,
        'uri',
        r'/bugs/(.*)'
    )

    # This enables creating a bug_ids key whether or not kwargs passes one in
    # needed by post(). Revisit need for this in future API revision.
    filters = [
        ("signatures", None, ["list", "str"]),
        ("bug_ids", None, ["list", "str"]),
    ]

    #--------------------------------------------------------------------------
    def __init__(self, config):
        self._impl = config.impl(config)
        #
        self._dispatch = {
            "signatures": self._impl.signatures,
            "bug_ids": self._impl.bug_ids
        }

    #--------------------------------------------------------------------------
    def get(self, **kwargs):
        import warnings
        warnings.warn("You should use the POST method to access bugs")
        return self.post(**kwargs)

    #--------------------------------------------------------------------------
    def post(self, **kwargs):
        """Return a list of signatures-to-bug_ids or bug_ids-to-signatures
           associations. """
        params = external_common.parse_arguments(self.filters, kwargs)

        if not params['signatures'] and not params['bug_ids']:
            raise MissingArgumentError('specify one of signatures or bug_ids')
        elif params['signatures'] and params['bug_ids']:
            raise BadArgumentError('specify only one of signatures or bug_ids')

        # there is one and only one item in the params mapping
        bugs = self._dispatch(params[0])

        return {
            "hits": bugs,
            "total": len(bugs)
        }
