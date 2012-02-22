import stat
try:
    import json
except ImportError:
    import simplejson as json
from socorro.lib import JsonDumpStorage
from socorro.lib.util import reportExceptionAndContinue
from socorro.external.crashstorage_base import (
  CrashStorageBase, OOIDNotFoundException)
from configman import Namespace


class CrashStorageForLocalFS(CrashStorageBase):

    required_config = Namespace()
    required_config.add_option(
        'local_fs',
        doc='a path to a local file system',
        default='/home/socorro/primaryCrashStore'
    )
    required_config.add_option(
        'local_fs_dump_dir_count',
        doc='the number of dumps to be stored in a single directory in the '
            'local file system',
        default=1024
    )
    required_config.add_option(
        'local_fs_dump_gid',
        doc='the group ID for saved crashes in local file system (optional)',
        default=None
    )
    required_config.add_option(
        'local_fs_dump_permissions',
        doc='a number used for permissions crash dump files in the local '
            'file system',
        default=stat.S_IRGRP | stat.S_IWGRP | stat.S_IRUSR | stat.S_IWUSR
    )
    required_config.add_option(
        'local_fs_dir_permissions',
        doc='a number used for permissions for directories in the local '
            'file system',
        default=(stat.S_IRGRP | stat.S_IXGRP | stat.S_IWGRP | stat.S_IRUSR
                              | stat.S_IXUSR | stat.S_IWUSR)
    )

    required_config.add_option(
        'fallback_fs',
        doc='a path to a local file system to use if local store fails',
        default='/home/socorro/fallback',
    )
    required_config.add_option(
        'fallback_dump_dir_count',
        doc='the number of dumps to be stored in a single directory in the '
            'fallback file system',
        default=1024
    )
    required_config.add_option(
        'fallback_dump_gid',
        doc='the group ID for saved crashes in fallback file system (optional)',
        default=None
    )
    required_config.add_option(
        'fallback_dump_permissions',
        doc='a number used for permissions crash dump files in the fallback '
            'file system',
        default=stat.S_IRGRP | stat.S_IWGRP | stat.S_IRUSR | stat.S_IWUSR
    )
    required_config.add_option(
        'fallback_dir_permissions',
        doc='a number used for permissions for directories in the fallback '
            'file system',
        default=(stat.S_IRGRP | stat.S_IXGRP | stat.S_IWGRP | stat.S_IRUSR
                              | stat.S_IXUSR | stat.S_IWUSR)
    )

    required_config.add_option(
        'json_file_suffix',
        doc='the suffix used to identify a json file',
        default='.json'
    )
    required_config.add_option(
        'dump_file_suffix',
        doc='the suffix used to identify a dump file',
        default='.dump'
    )

    def __init__(self, config):
        super(CrashStorageForLocalFS, self).__init__(config)
        #assert "localFS" in config, "localFS is missing from the configuration"
        #assert "localFSDumpPermissions" in config, "dumpPermissions is missing from the configuration"
        #assert "localFSDirPermissions" in config, "dirPermissions is missing from the configuration"
        #assert "localFSDumpDirCount" in config, "localFSDumpDirCount is missing from the configuration"
        #assert "localFSDumpGID" in config, "dumpGID is missing from the configuration"
        #assert "jsonFileSuffix" in config, "jsonFileSuffix is missing from the configuration"
        #assert "dumpFileSuffix" in config, "dumpFileSuffix is missing from the configuration"
        #assert "fallbackFS" in config, "fallbackFS is missing from the configuration"
        #assert "fallbackDumpDirCount" in config, "fallbackDumpDirCount is missing from the configuration"
        #assert "fallbackDumpGID" in config, "fallbackDumpGID is missing from the configuration"
        #assert "fallbackDumpPermissions" in config, "fallbackDumpPermissions is missing from the configuration"
        #assert "fallbackDirPermissions" in config, "fallbackDirPermissions is missing from the configuration"

        self.local_fs = JsonDumpStorage.JsonDumpStorage(
          root=config.local_fs,
          maxDirectoryEntries=config.local_fs_dump_dir_count,
          jsonSuffix=config.json_file_suffix,
          dumpSuffix=config.dump_file_suffix,
          dumpGID=config.local_fs_dump_gid,
          dumpPermissions=config.local_fs_dump_permissions,
          dirPermissions=config.local_fs_dir_permissions,
          logger=config.logger
        )

        self.fallbackFS = JsonDumpStorage.JsonDumpStorage(
          root=config.fallback_fs,
          maxDirectoryEntries=config.fallback_dump_dir_count,
          jsonSuffix=config.json_file_suffix,
          dumpSuffix=config.dump_file_suffix,
          dumpGID=config.fallback_dump_gid,
          dumpPermissions=config.fallback_dump_permissions,
          dirPermissions=config.fallback_dir_permissions,
          logger=config.logger
        )

    def save_raw(self, json_data, dump, use_fallback=False):
        try:
            ooid = json_data['ooid']
        except KeyError:
            raise OOIDNotFoundException("The json_data is always expected to "
                                        "have an 'ooid' key")
        try:
            fs = use_fallback and self.fallback_fs or self.local_fs
            jsonFileHandle, dumpFileHandle = fs.newEntry(
              ooid,
              self.hostname,
            )
            try:
                dumpFileHandle.write(dump)
                json.dump(json_data, jsonFileHandle)
            finally:
                dumpFileHandle.close()
                jsonFileHandle.close()
            self.logger.info('saved - %s', ooid)
            return self.OK
        except Exception:
            if use_fallback:
                self.logger.critical(
                  'fallback storage has failed: dropping %s on the floor',
                  ooid, exc_info=True)
            else:
                self.logger.warning(
                  'local storage has failed: trying fallback storage for: %s',
                  ooid, exc_info=True)
                return self.save_raw(json_data, dump, use_fallback=True)

        return self.ERROR

    def get_raw_json(self, ooid):
        job_pathname = self.local_fs.getJson(ooid)
        json_file = open(job_pathname)
        try:
            json_document = json.load(json_file)
        finally:
            json_file.close()
        return json_document

    def get_raw_dump(self, ooid):
        job_pathname = self.local_fs.getDump(ooid)
        dump_file = open(job_pathname)
        try:
            binary = dump_file.read()
        finally:
            dump_file.close()
        return binary

    def new_ooids(self):
        return self.local_fs.destructiveDateWalk()

    def remove(self, ooid):
        self.local_fs.remove(ooid)


class CrashStorageForNFS(CrashStorageBase):

    required_config = Namespace()
    required_config.add_option('', default='???')
    required_config.add_option('storage_root', default='???')
    required_config.add_option('dump_dir_count', default='???')
    required_config.add_option('json_file_suffix', default='???')
    required_config.add_option('dump_file_suffix', default='???')
    required_config.add_option('dump_gid', default='???')
    required_config.add_option('dump_permissions', default='???')
    required_config.add_option('dir_permissions', default='???')
    required_config.add_option('deferred_storage_root', default='???')
    required_config.add_option('dump_dir_count', default='???')

    def __init__(self, config):
        super(CrashStorageForNFS, self).__init__(config)
        #assert "storageRoot" in config, "storageRoot is missing from the configuration"
        #assert "deferredStorageRoot" in config, "deferredStorageRoot is missing from the configuration"
        #assert "dumpPermissions" in config, "dumpPermissions is missing from the configuration"
        #assert "dirPermissions" in config, "dirPermissions is missing from the configuration"
        #assert "dumpGID" in config, "dumpGID is missing from the configuration"
        #assert "jsonFileSuffix" in config, "jsonFileSuffix is missing from the configuration"
        #assert "dumpFileSuffix" in config, "dumpFileSuffix is missing from the configuration"

        #self.throttler = LegacyThrottler(config)
        self.standard = JsonDumpStorage.JsonDumpStorage(
          root=config.storage_root,
          maxDirectoryEntries=config.dump_dir_count,
          jsonSuffix=config.json_file_suffix,
          dumpSuffix=config.dump_file_suffix,
          dumpGID=config.dump_gid,
          dumpPermissions=config.dump_permissions,
          dirPermissions=config.dir_permissions,
        )
        self.deferred = JsonDumpStorage.JsonDumpStorage(
          root=config.deferred_storage_root,
          maxDirectoryEntries=config.dump_dir_count,
          jsonSuffix=config.json_file_suffix,
          dumpSuffix=config.dump_file_suffix,
          dumpGID=config.dump_gid,
          dumpPermissions=config.dump_permissions,
          dirPermissions=config.dir_permissions,
        )

    def save_raw(self, ooid, jsonData, dump):
        try:
            #throttleAction = self.throttler.throttle(jsonData)
            #throttleAction = jsonData.legacy_processing
            #if throttleAction == LegacyThrottler.DISCARD:
            #    self.logger.debug("discarding %s %s",
            #                      jsonData.ProductName,
            #                      jsonData.Version)
            #    return self.DISCARDED
            #elif throttleAction == LegacyThrottler.DEFER:
            #    self.logger.debug("deferring %s %s",
            #                      jsonData.ProductName,
            #                      jsonData.Version)
            #    fileSystemStorage = self.deferredFileSystemStorage
            #else:
            #    self.logger.debug("not throttled %s %s",
            #                      jsonData.ProductName,
            #                      jsonData.Version)
            #    fileSystemStorage = self.standardFileSystemStorage
            jsonFileHandle, dumpFileHandle = self.standard.newEntry(
              ooid,
              self.hostname
            )
            try:
                dumpFileHandle.write(dump)
                json.dump(json_data, jsonFileHandle)
            finally:
                dumpFileHandle.close()
                jsonFileHandle.close()
            return self.OK
        except:
            self.logger.error("Failed to set up NFS file storage", exc_info=True)
            return self.ERROR

    def get_raw_json(self, ooid):
        pathname = self._jsonPathForOoidInJsonDumpStorage(ooid)
        json_file = open(jopathname)
        try:
            return json.load(jsonFile)
        finally:
            jsonFile.close()

    def has_ooid(self, ooid):
        try:
            ooidPath = self.standard.getJson(ooid)  # XXX bug?
            self.standard.markAsSeen(ooid)
        except (OSError, IOError):
            try:
                ooidPath = self.deferred.getJson(ooid)  # XXX bug?
                self.deferred.markAsSeen(ooid)
            except (OSError, IOError):
                return False
        return True

    def new_ooids(self):
        return self.standardJobStorage.destructiveDateWalk()


    ## private methods --------------------------------------------------------

    def _jsonPathForOoidInJsonDumpStorage(self, ooid):
        try:
            return self.standard.getJson(ooid)
        except (OSError, IOError):
            try:
                return self.deferred.getJson(ooid)
            except (OSError, IOError):
                raise OOIDNotFoundException(
                   "%s cannot be found in standard or deferred storage" % ooid
                )

    def dumpPathForOoid(self, ooid, ignoredBasePath):
        try:
            return self.standardJobStorage.getDump(ooid)
        except (OSError, IOError):
            try:
                return self.deferredJobStorage.getDump(ooid)
            except (OSError, IOError):
                raise OOIDNotFoundException(
                   "%s cannot be found in standard or deferred storage" % ooid
                )

    def cleanUpTempDumpStorage(self, ooid, ignoredBasePath):
        pass
