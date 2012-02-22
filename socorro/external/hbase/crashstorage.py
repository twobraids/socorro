import os
import base64
try:
    import json
except ImportError:
    import simplejson as json
from socorro.external.crashstorage_base import CrashStorageSystem
from socorro.storage import hbaseClient
from socorro.lib import JsonDumpStorage
from socorro.lib.util import reportExceptionAndContinue


class CrashStorageSystemForHBase(CrashStorageSystem):
    def __init__(self, config, configPrefix='',
                 hbase_client=hbaseClient):
        super(CrashStorageSystemForHBase, self).__init__(config)

        self.logger.info('connecting to hbase')
        if not configPrefix:
            self.hbaseConnection = hbase_client.HBaseConnectionForCrashReports(
                config.hbaseHost,
                config.hbasePort,
                config.hbaseTimeout,
                logger=self.logger)
        else:
            hbaseHost = '%s%s' % (configPrefix, 'HbaseHost')
            #assert hbaseHost in config, "%s is missing from the configuration" % hbaseHost
            hbasePort = '%s%s' % (configPrefix, 'HbasePort')
            #assert hbasePort in config, "%s is missing from the configuration" % hbasePort
            hbaseTimeout = '%s%s' % (configPrefix, 'HbaseTimeout')
            #assert hbaseTimeout in config, "%s is missing from the configuration" % hbaseTimeout
            self.hbaseConnection = hbase_client.HBaseConnectionForCrashReports(
                config[hbaseHost],
                config[hbasePort],
                config[hbaseTimeout],
                logger=self.logger)
        retry_exceptions_list = \
          list(self.hbaseConnection.hbaseThriftExceptions)
        retry_exceptions_list.append(hbaseClient.NoConnectionException)
        self.exceptionsEligibleForRetry = tuple(retry_exceptions_list)

    def close(self):
        self.hbaseConnection.close()

    def save_raw(self, uuid, jsonData, dump, currentTimestamp=None):
        try:
            #jsonDataAsString = json.dumps(jsonData)
            self.hbaseConnection.put_json_dump(
              uuid, jsonData, dump, number_of_retries=2)
            self.logger.info('saved - %s', uuid)
            return CrashStorageSystem.OK
        except self.exceptionsEligibleForRetry:
            reportExceptionAndContinue(self.logger)
            return CrashStorageSystem.RETRY
        except Exception:
            reportExceptionAndContinue(self.logger)
            return CrashStorageSystem.ERROR

    def save_processed(self, uuid, jsonData):
        self.hbaseConnection.put_processed_json(
          uuid, jsonData, number_of_retries=2)

    def get_meta(self, uuid):
        return self.hbaseConnection.get_json(uuid, number_of_retries=2)

    def get_raw_dump(self, uuid):
        return self.hbaseConnection.get_dump(uuid, number_of_retries=2)

    def get_raw_dump_base64(self, uuid):
        dump = self.get_raw_dump(uuid, number_of_retries=2)
        return base64.b64encode(dump)

    def get_processed(self, uuid):
        return self.hbaseConnection.get_processed_json(
          uuid, number_of_retries=2)

    def uuidInStorage(self, uuid):
        return self.hbaseConnection.acknowledge_ooid_as_legacy_priority_job(
          uuid, number_of_retries=2)

    def dumpPathForUuid(self, uuid, basePath):
        dumpPath = ("%s/%s.dump" % (basePath, uuid)).replace('//', '/')
        f = open(dumpPath, "w")
        try:
            dump = self.hbaseConnection.get_dump(uuid, number_of_retries=2)
            f.write(dump)
        finally:
            f.close()
        return dumpPath

    def cleanUpTempDumpStorage(self, uuid, basePath):
        dumpPath = ("%s/%s.dump" % (basePath, uuid)).replace('//', '/')
        os.unlink(dumpPath)

    def newUuids(self):
        return self.hbaseConnection.iterator_for_all_legacy_to_be_processed()


class DualHbaseCrashStorageSystem(CrashStorageSystemForHBase):
    def __init__(self, config,
                 hbase_client=hbaseClient,
                 json_dump_storage=JsonDumpStorage):
        super(DualHbaseCrashStorageSystem, self).__init__(
          config,
          hbase_client=hbase_client,
          json_dump_storage=json_dump_storage
        )

        self.fallbackHBase = CrashStorageSystemForHBase(
          config,
          configPrefix='secondary',
          hbase_client=hbase_client,
          json_dump_storage=json_dump_storage
        )

    def get_meta(self, uuid):
        try:
            return self.hbaseConnection.get_json(uuid, number_of_retries=2)
        except hbaseClient.OoidNotFoundException:
            return self.fallbackHBase.get_meta(uuid)

    def get_raw_dump(self, uuid):
        try:
            return self.hbaseConnection.get_dump(uuid, number_of_retries=2)
        except hbaseClient.OoidNotFoundException:
            return self.fallbackHBase.get_raw_dump(uuid)

    def get_processed(self, uuid):
        try:
            return self.hbaseConnection.get_processed_json(
              uuid, number_of_retries=2)
        except hbaseClient.OoidNotFoundException:
            return self.fallbackHBase.get_processed(uuid)


class CollectorCrashStorageSystemForHBase(CrashStorageSystemForHBase):

    def __init__(self, config,
                 hbase_client=hbaseClient,
                 json_dump_storage=JsonDumpStorage):
        super(CollectorCrashStorageSystemForHBase, self).__init__(
          config,
          hbase_client=hbase_client,
          json_dump_storage=json_dump_storage
        )
        if config.hbaseFallbackFS:
            self.fallbackCrashStorage = json_dump_storage.JsonDumpStorage(
              root=config.hbaseFallbackFS,
              maxDirectoryEntries=config.hbaseFallbackDumpDirCount,
              jsonSuffix=config.jsonFileSuffix,
              dumpSuffix=config.dumpFileSuffix,
              dumpGID=config.hbaseFallbackDumpGID,
              dumpPermissions=config.hbaseFallbackDumpPermissions,
              dirPermissions=config.hbaseFallbackDirPermissions,
              logger=config.logger,
              )
        else:
            self.fallbackCrashStorage = None

    def save_raw(self, uuid, jsonData, dump, currentTimestamp):
        try:
            #jsonDataAsString = json.dumps(jsonData)
            self.hbaseConnection.put_json_dump(
              uuid, jsonData, dump, number_of_retries=2)
            return CrashStorageSystem.OK
        except Exception:
            reportExceptionAndContinue(self.logger)
            if self.fallbackCrashStorage:
                self.logger.warning('cannot save %s in hbase, falling back to '
                                    'filesystem', uuid)
                try:
                    jsonFileHandle, dumpFileHandle = \
                      self.fallbackCrashStorage.newEntry(uuid,
                                                         self.hostname,
                                                         currentTimestamp)
                    try:
                        dumpFileHandle.write(dump)
                        json.dump(jsonData, jsonFileHandle)
                    finally:
                        dumpFileHandle.close()
                        jsonFileHandle.close()
                    return CrashStorageSystem.OK
                except Exception:
                    reportExceptionAndContinue(self.logger)
            else:
                self.logger.warning('there is no fallback storage for hbase: '
                                    'dropping %s on the floor', uuid)
            return CrashStorageSystem.ERROR
