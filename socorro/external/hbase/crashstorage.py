from socorro.external.crashstorage_base import (
  CrashStorageBase, OOIDNotFoundException)
from socorro.storage import hbaseClient
from configman import Namespace


class HBaseCrashStorage(CrashStorageBase):

    required_config = Namespace()
    required_config.add_option(
        'number_of_retries',
        doc='Max. number of retries when fetching from hbaseClient',
        default=2
    )
    required_config.add_option(
        'hbase_host',
        doc='Host to HBase server',
        default='localhost',
    )
    required_config.add_option(
        'hbase_port',
        doc='Port to HBase server',
        default=9090,
    )
    required_config.add_option(
        'hbase_timeout',
        doc='timeout in milliseconds for an HBase connection',
        default=5000,
    )

    def __init__(self, config):
        super(HBaseCrashStorage, self).__init__(config)

        self.logger.info('connecting to hbase')
        self.hbaseConnection = hbaseClient.HBaseConnectionForCrashReports(
            config.hbase_host,
            config.hbase_port,
            config.hbase_timeout,
            logger=self.logger
        )

        self.exceptionsEligibleForRetry += \
          self.hbaseConnection.hbaseThriftExceptions
        self.exceptionsEligibleForRetry += (hbaseClient.NoConnectionException,)

    def close(self):
        self.hbaseConnection.close()

    def save_raw(self, json_data, dump):
        try:
            ooid = json_data['ooid']
        except KeyError:
            raise OOIDNotFoundException("The json_data is always expected to "
                                        "have an 'ooid' key")
        try:
            assert json_data['submitted_timestamp']
        except KeyError:
            raise ValueError("data must contain a 'submitted_timestamp' key")

        try:
            self.hbaseConnection.put_json_dump(
              ooid, json_data, dump,
              number_of_retries=self.config.number_of_retries)
            self.logger.info('saved - %s', ooid)
            return self.OK
        except self.exceptionsEligibleForRetry:
            self.logger.error("Exception eligable for retry", exc_info=True)
            return self.RETRY
        except Exception:
            self.logger.error("Other error on put_json_dump", exc_info=True)
            return self.ERROR

    def save_processed(self, ooid, json_data):
        self.hbaseConnection.put_processed_json(ooid, json_data,
                               number_of_retries=self.config.number_of_retries)

    def get_raw_json(self, ooid):
        return self.hbaseConnection.get_json(ooid,
                               number_of_retries=self.config.number_of_retries)

    def get_raw_dump(self, ooid):
        return self.hbaseConnection.get_dump(ooid,
                               number_of_retries=self.config.number_of_retries)

    def get_processed_json(self, ooid):
        return self.hbaseConnection.get_processed_json(ooid,
                               number_of_retries=self.config.number_of_retries)

    def new_ooids(self):
        return self.hbaseConnection.iterator_for_all_legacy_to_be_processed()

## Processors should do this stuff instead
#    def dumpPathForUuid(self, uuid, basePath):
#        dumpPath = ("%s/%s.dump" % (basePath, uuid)).replace('//', '/')
#        f = open(dumpPath, "w")
#        try:
#            dump = self.hbaseConnection.get_dump(uuid, number_of_retries=2)
#            f.write(dump)
#        finally:
#            f.close()
#        return dumpPath
#
#    def cleanUpTempDumpStorage(self, uuid, basePath):
#        dumpPath = ("%s/%s.dump" % (basePath, uuid)).replace('//', '/')
#        os.unlink(dumpPath)
