# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from socorro.external import MissingArgumentError, ResourceNotFound, \
                             ResourceUnavailable
from socorro.external.crashstorage_base import CrashIDNotFound
from socorro.lib import external_common

class CrashDataBase(object):

    """
    Implement the /crash_data service with the file system.
    """

    def __init__(self, *args, **kwargs):
        super(CrashDataBase, self).__init__()
        self.config = kwargs['config']
        self.all_services = kwargs['all_services']

    def get_storage(self):
        raise NotImplementedError

    def get(self, **kwargs):
        """Return JSON data of a crash report, given its uuid. """
        filters = [
            ('uuid', None, 'str'),
            ('datatype', None, 'str')
        ]
        params = external_common.parse_arguments(filters, kwargs)
        print kwargs

        if not params.uuid:
            raise MissingArgumentError('uuid')

        if not params.datatype:
            raise MissingArgumentError('datatype')

        print 'befor'
        store = self.get_storage()
        print 'after'

        datatype_method_mapping = {
            'raw': 'get_raw_dump',
            'meta': 'get_raw_crash',
            'processed': 'get_processed',
            'unredacted': 'get_unredacted_processed',
        }

        get = store.__getattribute__(datatype_method_mapping[params.datatype])
        print "TTTT", params.datatype
        print "SSSS", datatype_method_mapping[params.datatype]
        print "UUUU", get
        try:
            print 'sally'
            if params.datatype == 'raw':
                print 'wrong'
                return (get(params.uuid), 'application/octet-stream')
            else:
                print 'totally fuckud'
                return get(params.uuid)
        except CrashIDNotFound:
            if params.datatype in ('processed', 'unredacted'):
                #self.get(datatype='raw', uuid=params.uuid)
                for url, service_implementation in self.all_services:
                    if 'priorityjobs' in url:
                        j = service_implementation(config=self.config)
                        print j
                        j.create(uuid=params.uuid)
                        raise ResourceUnavailable(params.uuid)
            raise ResourceNotFound(params.uuid)
