"""This is the base of the crashstorage system - a unified interfaces for
saving, fetching and iterating over raw crashes, dumps and processed crashes.
"""

import os
from configman import Namespace,  RequiredConfig
from configman.converters import classes_in_namespaces_converter, \
                                 class_converter
from configman.dotdict import DotDict


#==============================================================================
class OOIDNotFoundException(Exception):
    pass


#==============================================================================
class CrashStorageBase(RequiredConfig):
    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        self.config = config
        if quit_check_callback:
            self.quit_check = quit_check_callback
        else:
            self.quit_check = lambda: False
        self.logger = config.logger
        self.exceptionsEligibleForRetry = ()

    #--------------------------------------------------------------------------
    def close(self):
        raise NotImplementedError("close() is not implemented")

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_json, dump):
        #assert isinstance(raw_json, dict)
        #assert raw_json.get('ooid')
        pass

    #--------------------------------------------------------------------------
    def save_processed(self, processed_json):
        #assert isinstance(raw_json, dict)
        #assert raw_json.get('ooid')
        pass

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, raw_json, dump, processed_json):
        self.save_raw_crash(raw_json, dump)
        self.save_processed(processed_json)

    #--------------------------------------------------------------------------
    def get_raw_json(self, ooid):
        raise NotImplementedError("get_raw_json is not implemented")

    #--------------------------------------------------------------------------
    def get_raw_dump(self, ooid):
        raise NotImplementedError("get_raw_crash is not implemented")

    #--------------------------------------------------------------------------
    def get_processed_json(self, ooid):
        raise NotImplementedError("get_processed is not implemented")

    #--------------------------------------------------------------------------
    def remove(self, ooid):
        raise NotImplementedError("remove is not implemented")

    #--------------------------------------------------------------------------
    def new_ooids(self):
        """returns an iterator of OOIDs that are considered new.

        New means OOIDs that have not been processed before. (NEEDS MORE LOVE)
        """
        raise StopIteration


#==============================================================================
class PolyCrashStorage(CrashStorageBase):
    required_config = Namespace()
    required_config.add_option(
      'storage_classes',
      doc='a comma delimited list of storage classes',
      default='',
      from_string_converter=classes_in_namespaces_converter(
          template_for_namespace='storage%d',
          name_of_class_option='store',
          instantiate_classes=False,
      )
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback):
        super(PolyCrashStorage, self).__init__(config, quit_check_callback)
        self.storage_namespaces = \
          config.storage_classes.subordinate_namespace_names
        self.stores = DotDict()
        for a_namespace in self.storage_namespaces:
            self.stores[a_namespace] = \
              config[a_namespace].store(config[a_namespace])

    #--------------------------------------------------------------------------
    def close(self):
        for a_store in self.stores:
            try:
                a_store.close()
            except NotImplemented:
                pass

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_json, dump):
        for a_store in self.stores:
            self.quit_check_callback()
            result = a_store.save_raw_crash(raw_json, dump)
            if self.ERROR in result or self.RETRY in result:
                return self.ERROR
        return self.OK

    #--------------------------------------------------------------------------
    def save_processed(self, processed_json):
        for a_store in self.stores:
            self.quit_check_callback()
            a_store.save_raw_crash(raw_json, dump)

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, raw_json, dump, processed_json):
        for a_store in self.stores:
            a_store.save_raw_and_processed(raw_json, dump, processed_json)


#==============================================================================
class FallbackCrashStorage(CrashStorageBase):
    required_config = Namespace()
    required_config.primary = Namespace()
    required_config.primary.add_option(
      'storage_class',
      doc='storage class for primary storage',
      default='',
      from_string_converter=class_converter
    )
    required_config.fallback = Namespace()
    required_config.fallback.add_option(
      'storage_class',
      doc='storage class for fallback storage',
      default='',
      from_string_converter=class_converter
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback):
        super(FallbackCrashStorage, self).__init__(config, quit_check_callback)
        self.primary_store = config.primary.storage_class(config.primary)
        self.fallback_store = config.fallback.storage_class(config.fallback)
        self.logger = self.config._root.logger


    #--------------------------------------------------------------------------
    def close(self):
        for a_store in (self.primary_store, self.fallback_store):
            try:
                a_store.close()
            except NotImplemented:
                pass

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_json, dump):
        try:
            return self.primary_store.save_raw_crash(raw_json, dump)
        except Exception:
            self.logger.critical('error in saving primary', exc_info=True)
            try:
                return self.fallback_store.save_raw_crash(raw_json, dump)
            except Exception:
                self.logger.critical('error in saving fallback', exc_info=True)
        return ERROR

    #--------------------------------------------------------------------------
    def save_processed(self, processed_json):
        try:
            self.primary_store.save_processed(processed_json)
        except Exception:
            self.logger.critical('error in saving primary', exc_info=True)
            try:
                self.fallback_store.save_processed(processed_json)
            except Exception:
                self.logger.critical('error in saving fallback', exc_info=True)


