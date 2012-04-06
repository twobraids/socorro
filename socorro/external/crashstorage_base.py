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
    """the base class for all crash storage classes"""
    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        """base class constructor

        parameters:
            config - a configman dot dict holding configuration information
            quit_check_callback - a function to be called periodically during
                                  long running operations.  It should check
                                  whatever the client app uses to detect a
                                  quit request and raise a KeyboardInterrupt.
                                  All derived classes should be prepared to
                                  shut down cleanly on getting such an
                                  exception from a call to this function

        instance varibles:
            self.config - a reference to the config mapping
            self.quit_check - a reference to the quit detecting callback
            self.logger - convience shortcut to the logger in the config
            self.exceptions_eligible_for_retry - a collection of non-fatal
                    exceptions that can be raised by a given storage
                    implementation.  This may be fetched by a client of the
                    crashstorge so that it can determine if it can try a failed
                    storage operation again."""
        self.config = config
        if quit_check_callback:
            self.quit_check = quit_check_callback
        else:
            self.quit_check = lambda: False
        self.logger = config.logger
        self.exceptions_eligible_for_retry = ()

    #--------------------------------------------------------------------------
    def close(self):
        """some implementations may need explicit closing.  All storage classes
        must override this function or suffer this exception"""
        raise NotImplementedError("close() is not implemented")

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dump):
        """this method that saves  both the raw_crash (sometimes called the
        raw_json) and the dump, must be overridden in any implementation.

        Why is does this base implementation just silently do nothing rather
        than raise a NotImplementedError?  Implementations of crashstorage
        are not required to implement the entire api.  Some may save only
        processed crashes but may be bundled (see the PolyCrashStorage class)
        with other crashstorage implementations.  Rather than having a non-
        implenting class raise an exeception that would derail the other
        bundled operations, the non-implementing storageclass will just
        quietly do nothing.

        parameters:
            raw_crash - a mapping containing the raw crash meta data.  It is
                        often saved as a json file, but here it is in the form
                        of a dict.
            dump - a binary blob of data that will eventually fed to minidump-
                   stackwalk"""
        pass

    #--------------------------------------------------------------------------
    def save_processed(self, processed_crash):
        """this method saves the processed_crash and must be overridden in
        anything that chooses to implement it.

        Why is does this base implementation just silently do nothing rather
        than raise a NotImplementedError?  Implementations of crashstorage
        are not required to implement the entire api.  Some may save only
        processed crashes but may be bundled (see the PolyCrashStorage class)
        with other crashstorage implementations.  Rather than having a non-
        implenting class raise an exeception that would derail the other
        bundled operations, the non-implementing storageclass will just
        quietly do nothing.

        parameters:
            processed_crash - a mapping contianing the processed crash"""
        pass

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, raw_crash, dump, processed_crash):
        """Mainly for the convenience and efficiency of the processor,
        this unified method combines saving both raw and processed crashes.

        parameters:
            raw_crash - a mapping containing the raw crash meta data.  It is
                        often saved as a json file, but here it is in the form
                        of a dict.
             dump - a binary blob of data that will eventually fed to minidump-
                    stackwalk
            processed_crash - a mapping contianing the processed crash"""
        self.save_raw_crash(raw_crash, dump)
        self.save_processed(processed_crash)

    #--------------------------------------------------------------------------
    def get_raw_crash(self, ooid):
        """the default implemntation of fetching a raw_crash

        parameters:
           ooid - the id of a raw crash to fetch"""
        raise NotImplementedError("get_raw_crash is not implemented")

    #--------------------------------------------------------------------------
    def get_raw_dump(self, ooid):
        """the default implemntation of fetching a dump

        parameters:
           ooid - the id of a dump to fetch"""
        raise NotImplementedError("get_raw_dump is not implemented")

    #--------------------------------------------------------------------------
    def get_processed_crash(self, ooid):
        """the default implemntation of fetching a processed_crash

        parameters:
           ooid - the id of a processed_crash to fetch"""
        raise NotImplementedError("get_processed_crash is not implemented")

    #--------------------------------------------------------------------------
    def remove(self, ooid):
        """delete a crash from storage

        parameters:
           ooid - the id of a crash to fetch"""
        raise NotImplementedError("remove is not implemented")

    #--------------------------------------------------------------------------
    def new_ooids(self):
        """a generator handing out a sequence of ooids of crashes that are
        considered to be new.  Each implementation can interpret the concept
        of "new" in an implementation specific way.  To be useful, derived
        class ought to override this method.
        """
        raise StopIteration

#==============================================================================
class PolyError(Exception):
    def __init__(self, exception_collection):
        self.exceptions = exception_collection


#==============================================================================
class PolyCrashStorage(CrashStorageBase):
    """a crashstorage implementation that encapsulates a collection of other
    crashstorage instances.  Any save operation applied to an instance of this
    class will be applied to all the crashstorge in the collection.

    The contianed crashstorage instances are specified in the configuration.
    Each class specified in the 'storage_classes' config option will be given
    its own numbered namespace in the form 'storage%d'.  With in the namespace,
    the class itself will be referred to as just 'store'.  Any configuration
    requirements within the class 'store' will be isolated within the local
    namespace.  That allows multiple instances of the same storageclass to
    avoid name collisions.
    """
    required_config = Namespace()
    required_config.add_option(
      'storage_classes',
      doc='a comma delimited list of storage classes',
      default='',
      from_string_converter=classes_in_namespaces_converter(
          template_for_namespace='storage%d',
          name_of_class_option='store',
          instantiate_classes=False,  # we instantiate manually for thread
                                      # safety
      )
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback):
        """instantiate all the subordinate crashstorage instances

        parameters:
            config - a configman dot dict holding configuration information
            quit_check_callback - a function to be called periodically during
                                  long running operations.

        instance variables:
            self.storage_namespaces - the list of the namespaces inwhich the
                                      subordinate instances are stored.
            self.stores - instances of the subordinate crash stores

        """
        super(PolyCrashStorage, self).__init__(config, quit_check_callback)
        self.storage_namespaces = \
          config.storage_classes.subordinate_namespace_names
        self.stores = DotDict()
        for a_namespace in self.storage_namespaces:
            self.stores[a_namespace] = \
              config[a_namespace].store(config[a_namespace])

    #--------------------------------------------------------------------------
    def close(self):
        """iterate through the subordinate crash stores and close them"""
        for a_store in self.stores:
            try:
                a_store.close()
            except NotImplemented:
                pass

    #--------------------------------------------------------------------------
    def save_raw_crash(self, raw_crash, dump):
        """iterate through the subordinate crash stores saving the raw_crash
        and the dump to each of them.

        parameters:
            raw_crash - the meta data mapping
            dump - the raw binary crash data"""
        exceptions = []
        for a_store in self.stores:
            self.quit_check_callback()
            try:
                a_store.save_raw_crash(raw_crash, dump)
            except Exception, x:
                exceptions.append(x)
        if exceptions:
            raise PolyError(exceptions)


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


