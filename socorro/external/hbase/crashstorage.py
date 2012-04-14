from socorro.external.crashstorage_base import (
  CrashStorageBase, OOIDNotFoundException)
from socorro.external.hbase import hbase_client
from socorro.database.transaction_executor import TransactionExecutor
from configman import Namespace, class_converter


#==============================================================================
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
    required_config.add_option(
      'transaction_executor_class',
      default=TransactionExecutor,
      doc='a class that will execute transactions',
      from_string_converter=class_converter)

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(HBaseCrashStorage, self).__init__(config, quit_check_callback)

        self.logger.info('connecting to hbase')
        self.hbaseConnection = hbase_client.HBaseConnectionForCrashReports(
          config.hbase_host,
          config.hbase_port,
          config.hbase_timeout,
          logger=self.logger
        )

        self.transaction_executor = config.transaction_executor_class(
          config,
          self.hbaseConnection,
          self.quit_check
        )

        self.exceptions_eligible_for_retry += \
          self.hbaseConnection.hbaseThriftExceptions
        self.exceptions_eligible_for_retry += \
          (hbase_client.NoConnectionException,)

    #--------------------------------------------------------------------------
    def close(self):
        self.hbaseConnection.close()

    #--------------------------------------------------------------------------
    def save_raw_crash(self, json_data, dump):
        try:
            ooid = json_data['ooid']
        except KeyError:
            raise OOIDNotFoundException("The json_data is always expected to "
                                        "have an 'ooid' key")
        try:
            assert json_data['submitted_timestamp']
        except KeyError:
            raise ValueError("data must contain a 'submitted_timestamp' key")

        in_context = self.transaction_executor(
          hbase_client.HBaseConnectionForCrashReports.put_json_dump,
          ooid,
          json_data,
          dump,
          #number_of_retries=self.config.number_of_retries
          number_of_retries=0
        )()

        self.logger.info('saved - %s', ooid)

    #--------------------------------------------------------------------------
    def save_processed(self, ooid, json_data):
        self.transaction_executor(
          hbase_client.HBaseConnectionForCrashReports.put_processed_json,
          ooid,
          json_data,
          number_of_retries=self.config.number_of_retries
        )
        #self.hbaseConnection.put_processed_json(ooid, json_data,
                               #number_of_retries=self.config.number_of_retries)

    #--------------------------------------------------------------------------
    def get_raw_crash(self, ooid):
        return self.transaction_executor(
          hbase_client.HBaseConnectionForCrashReports.get_json,
          ooid,
          number_of_retries=self.config.number_of_retries
        )
        #return self.hbaseConnection.get_json(ooid,
                               #number_of_retries=self.config.number_of_retries)

    #--------------------------------------------------------------------------
    def get_raw_dump(self, ooid):
        return self.transaction_executor(
          hbase_client.HBaseConnectionForCrashReports.get_dump,
          ooid,
          number_of_retries=self.config.number_of_retries
        )
        #return self.hbaseConnection.get_dump(ooid,
                               #number_of_retries=self.config.number_of_retries)

    #--------------------------------------------------------------------------
    def get_processed_crash(self, ooid):
        return self.transaction_executor(
          hbaseClient.HBaseConnectionForCrashReports.get_processed_json,
          ooid,
          number_of_retries=self.config.number_of_retries
        )
        #return self.hbaseConnection.get_processed_json(ooid,
                               #number_of_retries=self.config.number_of_retries)

    #--------------------------------------------------------------------------
    def new_ooids(self):
        # TODO: how do we put this is in a transactactional retry wrapper?
        return self.hbaseConnection.iterator_for_all_legacy_to_be_processed()


##==============================================================================
#class HBaseProcessorCrashStorage(HBaseCrashStorage):
    #"""This special version of HBaseCrashStorage is for use within the
    #processors.  At the end of processing a crash, the processors save
    #the raw crash, the dump and the processed crash in one call.  Hbase
    #already has the raw crash and the dump, it is inefficient to save
    #those two pieces over again.  see the details in the overridden method
    #below for details."""

    ##--------------------------------------------------------------------------
    #def save_raw_and_processed(self, raw_json, dump, processed_json):
        #"""In the processor, HBase alerady has the raw crash and the dump.
        #this overridden function prevents those values from being stored a
        #second time.

        #parameters:
        #raw_crash - the mapping holding the data from original json crash file.
                    #this mapping is ignored in this function
        #dump - the binary dump from the original crash - ignored by this method
        #processed_crash - a mapping that will become the processed_json saved
                          #into HBase."""
        #return self.save_processed_json(processed_json)


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
