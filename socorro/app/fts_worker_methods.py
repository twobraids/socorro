# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from socorro.external.crashstorage_base import (
    CrashStorageBase,
    CrashIDNotFound,
    NullCrashStorage
)
from inspect import getmembers, ismethod
from types import MethodType

from configman import Namespace, RequiredConfig
from functools import wraps


#==============================================================================
class RejectJob(Exception):
    pass


#------------------------------------------------------------------------------
def exception_wrapper(fn):
    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        try:
            return fn(self, *args, **kwargs)
        except CrashIDNotFound, x:
            message = "%r could not be found" % x
            self.config.logger.warning(message)
            raise RejectJob(message)
        except Exception as x:
            self.config.logger.error(
                "error reading raw_crash: %r",
                x,
                exc_info=True
            )
            raise
    return wrapper


#==============================================================================
class FTSWorkerMethodBase(CrashStorageBase):
    #
    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(
        self,
        config,
        fetch_store=None,
        save_store=None,
        transformation=None,
        quit_check=None
    ):
        super(FTSWorkerMethodBase, self).__init__(config)
        self.fetch_store = fetch_store if fetch_store else NullCrashStorage(config)
        self.save_store = save_store if save_store else NullCrashStorarage(config)
        self.transformation_fn = transformation
        self.quick_check = quit_check

    #--------------------------------------------------------------------------
    @exception_wrapper
    def save_raw_crash(self, raw_crash, dumps, crash_id):
        self.save_store.save_raw_crash(
            raw_crash,
            dumps,
            crash_id
        )

    #--------------------------------------------------------------------------
    @exception_wrapper
    def save_raw_crash_with_file_dumps(self, raw_crash, dumps, crash_id):
        self.save_store.save_raw_crash_with_file_dumps(
            raw_crash,
            dumps,
            crash_id
        )

    #--------------------------------------------------------------------------
    @exception_wrapper
    def save_processed(self, processed_crash):
        self.save_store.save_processed(processed_crash)

    #--------------------------------------------------------------------------
    @exception_wrapper
    def save_raw_and_processed(
        self,
        raw_crash,
        dumps,
        processed_crash,
        crash_id
    ):
        self.save_store.save_raw_and_processed(
            raw_crash,
            dumps,
            processed_crash,
            crash_id
        )

    #--------------------------------------------------------------------------
    @exception_wrapper
    def get_raw_crash(self, crash_id):
        return self.fetch_store.get_raw_crash(crash_id)

    #--------------------------------------------------------------------------
    @exception_wrapper
    def get_raw_dump(self, crash_id, name=None):
        return self.fetch_store.get_raw_crash(crash_id, name)

    #--------------------------------------------------------------------------
    @exception_wrapper
    def get_raw_dumps(self, crash_id):
        return self.fetch_store.get_raw_dumps(crash_id)

    #--------------------------------------------------------------------------
    @exception_wrapper
    def get_raw_dumps_as_files(self, crash_id):
        return self.fetch_store.get_raw_dumps_as_files(crash_id)

    #--------------------------------------------------------------------------
    @exception_wrapper
    def get_unredacted_processed(self, crash_id):
        return self.fetch_store.get_unredacted_processed(crash_id)

    #--------------------------------------------------------------------------
    @exception_wrapper
    def remove(self, crash_id):
        self.save_store.remove(crash_id)

    #--------------------------------------------------------------------------
    def __call__(self, crash_id):
        self.config.logger.info("starting job: %s", crash_id)
        try:
            self._call_impl(crash_id)
            self.config.logger.info("finished successful job: %s", crash_id)
        except Exception, x:
            self.config.logger.warning(
                'finished failed job: %s (%r)',
                crash_id,
                x
            )
            if isinstance(x, RejectJob):
                return
            raise


#==============================================================================
class RawCrashCopyWorkerMethod(FTSWorkerMethodBase):

    #--------------------------------------------------------------------------
    def _call_impl(self, crash_id):
        raw_crash = self.get_raw_crash(crash_id)
        raw_dumps = self.get_raw_dumps(crash_id)

        self.transformation_fn(raw_crash=raw_crash, raw_dumps=raw_dumps)

        self.save_raw_crash(raw_crash, raw_dumps, crash_id)


#==============================================================================
class RawCrashMoveWorkerMethod(RawCrashCopyWorkerMethod):

    #--------------------------------------------------------------------------
    def _call_impl(self, crash_id):
        super(RawCrashMoveWorkerMethod, self)._call_impl(crash_id)
        self.remove(crash_id)


#==============================================================================
class ProcessedCrashCopyWorkerMethod(FTSWorkerMethodBase):

    #--------------------------------------------------------------------------
    def _call_impl(self, crash_id):
        processed_crash = self.get_unredacted_processed(crash_id)

        self.transformation_fn(processed_crash=processed_crash)

        self.save_processed(processed_crash)


#==============================================================================
class CopyAllWorkerMethod(FTSWorkerMethodBase):

    #--------------------------------------------------------------------------
    def _call_impl(self, crash_id):
        raw_crash = self.get_raw_crash(crash_id)
        raw_dumps = self.get_raw_dumps(crash_id)
        processed_crash = self.get_unredacted_processed(crash_id)

        processed_crash = self.transformation_fn(
            raw_crash=raw_crash,
            raw_dumps=raw_crash,
            processed_crash=processed_crash
        )

        self.save_raw_and_processed(raw_crash, raw_dumps, processed_crash)


#==============================================================================
class ProcessorWorkerMethod(FTSWorkerMethodBase):

    #--------------------------------------------------------------------------
    def _call_impl(self, crash_id):
        raw_crash = self.get_raw_crash(crash_id)
        try:
            raw_dumps = self.get_raw_dumps_as_files(crash_id)
            processed_crash = self.get_unredacted_processed(crash_id)

            processed_crash = self.transformation_fn(
                raw_crash=raw_crash,
                raw_dumps=raw_dumps,
                processed_crash=processed_crash
            )

            self.save_raw_and_processed(
                raw_crash,
                None,
                processed_crash,
                crash_id
            )
        finally:
            raw_dumps.remove_temp_files()
