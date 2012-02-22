import re
import random
import time
import logging
import os
from socorro.lib.util import DotDict
from socorro.lib import ver_tools
from configman.config_manager import RequiredConfig
from configman import Namespace


class OOIDNotFoundException(Exception):
    pass


class CrashStorageBase(RequiredConfig):
    required_config = Namespace()

    NO_ACTION = 0
    OK = 1
    DISCARDED = 2
    ERROR = 3
    RETRY = 4

    def __init__(self, config):
        self.config = config
        # XXX isn't socket better for doing this?
        self.hostname = os.uname()[1]
        # XXX ideally refactor to just refer to self.config.logger
        self.logger = config.logger
        #try:
        #    if config.logger:
        #        self.logger = config.logger
        #    else:
        #        self.logger = logger
        #except KeyError:
        #    self.logger = logger
        self.exceptionsEligibleForRetry = []

    def close(self):
        pass

    def save_raw(self, raw_json, dump):
        #assert isinstance(raw_json, dict)
        #assert raw_json.get('ooid')
        return self.NO_ACTION

    def save_processed(self, processed_json):
        #assert isinstance(raw_json, dict)
        #assert raw_json.get('ooid')
        return self.NO_ACTION

    def get_raw_json(self, ooid):
        raise NotImplementedError("get_raw_json is not implemented")

    def get_raw_dump(self, ooid):
        raise NotImplementedError("get_raw_crash is not implemented")

    def get_processed_json(self, ooid):
        raise NotImplementedError("get_processed is not implemented")

    def remove(self, ooid):
        raise NotImplementedError("remove is not implemented")

    def has_ooid(self, ooid):
        """return true if the OOID exists indendent of content"""
        # this used to be called `uuidInStorage`
        raise NotImplementedError#return False

    def new_ooids(self):
        """returns an iterator of OOIDs that are considered new.

        New means OOIDs that have not been processed before. (NEEDS MORE LOVE)
        """
        raise StopIteration
