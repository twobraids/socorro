# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""This is the base of the crashstorage system - a unified interfaces for
saving, fetching and iterating over raw crashes, binary_symbols and processed crashes.
"""

import sys
import collections
import re

from configman import Namespace,  RequiredConfig
from configman.converters import classes_in_namespaces_converter, \
                                 class_converter
from configman.dotdict import DotDict


#==============================================================================
class SymbolIDNotFound(Exception):
    pass


#==============================================================================
class SymbolStorageBase(RequiredConfig):
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
        """some implementations may need explicit closing."""
        pass

    #--------------------------------------------------------------------------
    def save_symbol_meta(self, symbol_meta, binary_symbols, symbol_id):
        """this method that saves  both the symbol_meta and the dump, must be
        overridden in any implementation.

        Why is does this base implementation just silently do nothing rather
        than raise a NotImplementedError?  Implementations of crashstorage
        are not required to implement the entire api.  Some may save only
        processed crashes but may be bundled (see the PolySymbolStorage class)
        with other crashstorage implementations.  Rather than having a non-
        implenting class raise an exeception that would derail the other
        bundled operations, the non-implementing storageclass will just
        quietly do nothing.

        parameters:
            symbol_meta - a mapping containing the raw crash meta data.  It is
                        often saved as a json file, but here it is in the form
                        of a dict.
            binary_symbols - a dict of dump name keys and binary blob values
            symbol_id - the crash key to use for this crash"""
        pass

    #--------------------------------------------------------------------------
    def save_processed(self, processed_symbols):
        """this method saves the processed_symbols and must be overridden in
        anything that chooses to implement it.

        Why is does this base implementation just silently do nothing rather
        than raise a NotImplementedError?  Implementations of crashstorage
        are not required to implement the entire api.  Some may save only
        processed crashes but may be bundled (see the PolySymbolStorage class)
        with other crashstorage implementations.  Rather than having a non-
        implenting class raise an exeception that would derail the other
        bundled operations, the non-implementing storageclass will just
        quietly do nothing.

        parameters:
            processed_symbols - a mapping containing the processed crash"""
        pass

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, symbol_meta, binary_symbols, processed_symbols,
                               symbol_id):
        """Mainly for the convenience and efficiency of the processor,
        this unified method combines saving both raw and processed crashes.

        parameters:
            symbol_meta - a mapping containing the raw crash meta data. It is
                        often saved as a json file, but here it is in the form
                        of a dict.
            binary_symbols - a dict of dump name keys and binary blob values
            processed_symbols - a mapping containing the processed crash
            symbol_id - the crash key to use for this crash"""
        self.save_symbol_meta(symbol_meta, binary_symbols, symbol_id)
        self.save_processed(processed_symbols)

    #--------------------------------------------------------------------------
    def get_symbol_meta(self, symbol_id):
        """the default implementation of fetching a symbol_meta

        parameters:
           symbol_id - the id of a raw crash to fetch"""
        raise NotImplementedError("get_symbol_meta is not implemented")

    #--------------------------------------------------------------------------
    def get_symbol_binary(self, symbol_id, name=None):
        """the default implementation of fetching a dump

        parameters:
           symbol_id - the id of a dump to fetch
           name - the name of the dump to fetch"""
        raise NotImplementedError("get_symbol_binary is not implemented")

    #--------------------------------------------------------------------------
    def get_binary_symbols(self, symbol_id):
        """the default implementation of fetching all the binary_symbols

        parameters:
           symbol_id - the id of a dump to fetch"""
        raise NotImplementedError("get_binary_symbols is not implemented")

    #--------------------------------------------------------------------------
    def get_binary_symbols_as_files(self, symbol_id):
        """the default implementation of fetching all the binary_symbols as files on
        a file system somewhere.  returns a list of pathnames.

        parameters:
           symbol_id - the id of a dump to fetch"""
        raise NotImplementedError("get_binary_symbols is not implemented")

    #--------------------------------------------------------------------------
    def get_processed(self, symbol_id):
        """the default implementation of fetching a processed_symbols.  This
        method should not be overridden in subclasses unless the intent is to
        alter the redaction process.

        parameters:
           symbol_id - the id of a processed_symbols to fetch"""
        processed_symbols = self.get_unredacted_processed(symbol_id)
        self.redactor(processed_symbols)
        return processed_symbols

    #--------------------------------------------------------------------------
    def remove(self, symbol_id):
        """delete a crash from storage

        parameters:
           symbol_id - the id of a crash to fetch"""
        raise NotImplementedError("remove is not implemented")

    #--------------------------------------------------------------------------
    def new_symbols(self):
        """a generator handing out a sequence of symbol_ids of crashes that are
        considered to be new.  Each implementation can interpret the concept
        of "new" in an implementation specific way.  To be useful, derived
        class ought to override this method.
        """
        return []


#==============================================================================
class NullCrashStorage(SymbolStorageBase):
    """a testing crashstorage that silently ignores everything it's told to do
    """
    #--------------------------------------------------------------------------
    def get_symbol_meta(self, symbol_id):
        """the default implementation of fetching a symbol_meta

        parameters:
           symbol_id - the id of a raw crash to fetch"""
        return {}

    #--------------------------------------------------------------------------
    def get_symbol_binary(self, symbol_id, name):
        """the default implementation of fetching a dump

        parameters:
           symbol_id - the id of a dump to fetch"""
        return ''

    #--------------------------------------------------------------------------
    def get_binary_symbols(self, symbol_id):
        """the default implementation of fetching all the binary_symbols

        parameters:
           symbol_id - the id of a dump to fetch"""
        return {}

    #--------------------------------------------------------------------------
    def remove(self, symbol_id):
        """delete a crash from storage

        parameters:
           symbol_id - the id of a crash to fetch"""
        pass


#==============================================================================
class PolyStorageError(Exception, collections.MutableSequence):
    """an exception container holding a sequence of exceptions with tracebacks.

    parameters:
        message - an optional over all error message
    """
    def __init__(self, message=''):
        super(PolyStorageError, self).__init__(self, message)
        self.exceptions = []  # the collection

    def gather_current_exception(self):
        """append the currently active exception to the collection"""
        self.exceptions.append(sys.exc_info())

    def has_exceptions(self):
        """the boolean opposite of is_empty"""""
        return bool(self.exceptions)

    def __len__(self):
        """how many exceptions are stored?
        this method is required by the MutableSequence abstract base class"""
        return len(self.exceptions)

    def __iter__(self):
        """start an iterator over the squence.
        this method is required by the MutableSequence abstract base class"""
        return iter(self.exceptions)

    def __contains__(self, value):
        """search the sequence for a value and return true if it is present
        this method is required by the MutableSequence abstract base class"""

        return self.exceptions.__contains__(value)

    def __getitem__(self, index):
        """fetch a specific exception
        this method is required by the MutableSequence abstract base class"""
        return self.exceptions.__getitem__(index)

    def __setitem__(self, index, value):
        """change the value for an index in the sequence
        this method is required by the MutableSequence abstract base class"""
        self.exceptions.__setitem__(index, value)


#==============================================================================
class PolySymbolStorage(SymbolStorageBase):
    """a crashstorage implementation that encapsulates a collection of other
    crashstorage instances.  Any save operation applied to an instance of this
    class will be applied to all the crashstorge in the collection.

    This class is useful for 'save' operations only.  It does not implement
    the 'get' operations.

    The contained crashstorage instances are specified in the configuration.
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
          name_of_class_option='symbolstorage_class',
          instantiate_classes=False,  # we instantiate manually for thread
                                      # safety
      )
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
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
        super(PolySymbolStorage, self).__init__(config, quit_check_callback)
        self.storage_namespaces = \
          config.storage_classes.subordinate_namespace_names
        self.stores = DotDict()
        for a_namespace in self.storage_namespaces:
            self.stores[a_namespace] = \
              config[a_namespace].symbolstorage_class(
                                      config[a_namespace],
                                      quit_check_callback
                                 )

    #--------------------------------------------------------------------------
    def close(self):
        """iterate through the subordinate crash stores and close them.
        Even though the classes are closed in sequential order, all are
        assured to close even if an earlier one raises an exception.  When all
        are closed, any exceptions that were raised are reraised in a
        PolyStorageError

        raises:
          PolyStorageError - an exception container holding a list of the
                             exceptions raised by the subordinate storage
                             systems"""
        storage_exception = PolyStorageError()
        for a_store in self.stores.itervalues():
            try:
                a_store.close()
            except Exception, x:
                self.logger.error('%s failure: %s', a_store.__class__,
                                  str(x))
                storage_exception.gather_current_exception()
        if storage_exception.has_exceptions():
            raise storage_exception

    #--------------------------------------------------------------------------
    def save_symbol_meta(self, symbol_meta, binary_symbols, symbol_id):
        """iterate through the subordinate crash stores saving the symbol_meta
        and the dump to each of them.

        parameters:
            symbol_meta - the meta data mapping
            binary_symbols - a mapping of dump name keys to dump binary values
            symbol_id - the id of the crash to use"""
        storage_exception = PolyStorageError()
        for a_store in self.stores.itervalues():
            self.quit_check()
            try:
                a_store.save_symbol_meta(symbol_meta, binary_symbols, symbol_id)
            except Exception, x:
                self.logger.error('%s failure: %s', a_store.__class__,
                                  str(x))
                storage_exception.gather_current_exception()
        if storage_exception.has_exceptions():
            raise storage_exception

    #--------------------------------------------------------------------------
    def save_processed(self, processed_symbols):
        """iterate through the subordinate crash stores saving the
        processed_symbols to each of the.

        parameters:
            processed_symbols - a mapping containing the processed crash"""
        storage_exception = PolyStorageError()
        for a_store in self.stores.itervalues():
            self.quit_check()
            try:
                a_store.save_processed(processed_symbols)
            except Exception, x:
                self.logger.error('%s failure: %s', a_store.__class__,
                                  str(x), exc_info=True)
                storage_exception.gather_current_exception()
        if storage_exception.has_exceptions():
            raise storage_exception

    #--------------------------------------------------------------------------
    def save_raw_and_processed(self, symbol_meta, dump, processed_symbols,
                               symbol_id):
        for a_store in self.stores.itervalues():
            a_store.save_raw_and_processed(
              symbol_meta,
              dump,
              processed_symbols,
              symbol_id
            )


#==============================================================================
class FallbackSymbolStorage(SymbolStorageBase):
    """This storage system has a primary and fallback subordinate storage
    systems.  If an exception is raised by the primary storage system during
    an operation, the operation is repeated on the fallback storage system.

    This class is useful for 'save' operations only.  It does not implement
    the 'get' operations."""
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
    def __init__(self, config, quit_check_callback=None):
        """instantiate the primary and secondary storage systems"""
        super(FallbackSymbolStorage, self).__init__(config, quit_check_callback)
        self.primary_store = config.primary.storage_class(
            config.primary,
            quit_check_callback
        )
        self.fallback_store = config.fallback.storage_class(
            config.fallback,
            quit_check_callback
        )
        self.logger = self.config.logger

    #--------------------------------------------------------------------------
    def close(self):
        """close both storage systems.  The second will still be closed even
        if the first raises an exception. """
        poly_exception = PolyStorageError()
        for a_store in (self.primary_store, self.fallback_store):
            try:
                a_store.close()
            except NotImplementedError:
                pass
            except Exception:
                poly_exception.gather_current_exception()
        if len(poly_exception.exceptions) > 1:
            raise poly_exception

    #--------------------------------------------------------------------------
    def save_symbol_meta(self, symbol_meta, binary_symbols, symbol_id):
        """save raw crash data to the primary.  If that fails save to the
        fallback.  If that fails raise the PolyStorageException

        parameters:
            symbol_meta - the meta data mapping
            binary_symbols - a mapping of dump name keys to dump binary values
            symbol_id - the id of the crash to use"""
        try:
            self.primary_store.save_symbol_meta(symbol_meta, binary_symbols, symbol_id)
        except Exception:
            self.logger.critical('error in saving primary', exc_info=True)
            poly_exception = PolyStorageError()
            poly_exception.gather_current_exception()
            try:
                self.fallback_store.save_symbol_meta(symbol_meta, binary_symbols, symbol_id)
            except Exception:
                self.logger.critical('error in saving fallback', exc_info=True)
                poly_exception.gather_current_exception()
                raise poly_exception

    #--------------------------------------------------------------------------
    def save_processed(self, processed_symbols):
        """save processed crash data to the primary.  If that fails save to the
        fallback.  If that fails raise the PolyStorageException

        parameters:
            processed_symbols - a mapping containing the processed crash"""
        try:
            self.primary_store.save_processed(processed_symbols)
        except Exception:
            self.logger.critical('error in saving primary', exc_info=True)
            poly_exception = PolyStorageError()
            poly_exception.gather_current_exception()
            try:
                self.fallback_store.save_processed(processed_symbols)
            except Exception:
                self.logger.critical('error in saving fallback', exc_info=True)
                poly_exception.gather_current_exception()
                raise poly_exception

    #--------------------------------------------------------------------------
    def get_symbol_meta(self, symbol_id):
        """get a raw crash 1st from primary and if not found then try the
        fallback.

        parameters:
           symbol_id - the id of a raw crash to fetch"""
        try:
            return self.primary_store.get_symbol_meta(symbol_id)
        except SymbolIDNotFound:
            return self.fallback_store.get_symbol_meta(symbol_id)

    #--------------------------------------------------------------------------
    def get_symbol_binary(self, symbol_id, name=None):
        """get a named symbol file 1st from primary and if not found then try
        the fallback.

        parameters:
           symbol_id - the id of a dump to fetch"""
        try:
            return self.primary_store.get_symbol_binary(symbol_id, name)
        except SymbolIDNotFound:
            return self.fallback_store.get_symbol_binary(symbol_id, name)

    #--------------------------------------------------------------------------
    def get_binary_symbols(self, symbol_id):
        """get all crash binary_symbols 1st from primary and if not found then try
        the fallback.

        parameters:
           symbol_id - the id of a dump to fetch"""
        try:
            return self.primary_store.get_binary_symbols(symbol_id)
        except SymbolIDNotFound:
            return self.fallback_store.get_binary_symbols(symbol_id)

    #--------------------------------------------------------------------------
    def get_binary_symbols_as_files(self, symbol_id):
        """get all symbol file pathnames 1st from primary and if not found then
        try the fallback.

        parameters:
           symbol_id - the id of a dump to fetch"""
        try:
            return self.primary_store.get_binary_symbols_as_files(symbol_id)
        except SymbolIDNotFound:
            return self.fallback_store.get_binary_symbols_as_files(symbol_id)

    #--------------------------------------------------------------------------
    def remove(self, symbol_id):
        """delete a crash from storage

        parameters:
           symbol_id - the id of a crash to fetch"""
        try:
            self.primary_store.remove(symbol_id)
        except SymbolIDNotFound:
            self.fallback_store.remove(symbol_id)

    #--------------------------------------------------------------------------
    def new_symbols(self):
        """return an iterator that yields a list of symbol_ids of raw crashes
        that were added to the file system since the last time this iterator
        was requested."""
        for a_crash in self.fallback_store.new_symbols():
            yield a_crash
        for a_crash in self.primary_store.new_symbols():
            yield a_crash


