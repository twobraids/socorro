# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import functools
import hashlib
import json
import logging
import os
import stat
import time

from django.conf import settings
from django.core.cache import cache
from configman import (
    configuration,
    # ConfigFileFutureProxy,
    Namespace,
    environment
)
from configman.dotdict import DotDict
from socorro.app.socorro_app import App
from socorro.dataservice.util import (
    ServiceBase,
    classes_in_namespaces_converter,
)

# this logger is a problem.  While it uses the Pythonic standard for how
# loggers are setup, Socorro uses a different model.  Rather than relying
# on globals, Socorro uses configman to pass loggers between components.
# Since this logger was not created in that manner, it is never set up
# correctly to function properly.
logger = logging.getLogger('dataservice_models')


def memoize(function):
    """Decorator for model methods to cache in memory or the filesystem
    using CACHE_MIDDLEWARE and/or CACHE_MIDDLEWARE_FILES Django config"""

    @functools.wraps(function)
    def memoizer(instance, *args, **kwargs):

        def get_cached_result(key, instance, stringified_args):
            result = cache.get(key)
            if result is not None:
                logger.debug("CACHE HIT %s" % stringified_args)
                return result

            # Didn't find key in middleware_cache, so try filecache
            cache_file = get_cache_filename(key, instance)
            if settings.CACHE_MIDDLEWARE_FILES and os.path.isfile(cache_file):
                # but is it fresh enough?
                age = time.time() - os.stat(cache_file)[stat.ST_MTIME]
                if age > instance.cache_seconds:
                    logger.debug("CACHE FILE TOO OLD")
                    os.remove(cache_file)
                else:
                    logger.debug("CACHE FILE HIT %s" % stringified_args)
                    if instance.expect_json:
                        return json.load(open(cache_file))
                    else:
                        return open(cache_file).read()

            # Didn't find our values in the cache
            return None

        def get_cache_filename(key, instance):
            root = settings.CACHE_MIDDLEWARE_FILES
            if isinstance(root, bool):
                cache_file = os.path.join(
                    settings.ROOT,
                    'models-cache'
                )
            else:
                cache_file = root

            cache_file = os.path.join(cache_file, classname, key)
            cache_file += instance.expect_json and '.json' or '.dump'
            return cache_file

        def refresh_caches(key, instance, result):
            cache.set(key, result, instance.cache_seconds)
            cache_file = get_cache_filename(key, instance)
            if cache_file and settings.CACHE_MIDDLEWARE_FILES:
                if not os.path.isdir(os.path.dirname(cache_file)):
                    os.makedirs(os.path.dirname(cache_file))
                with open(cache_file, 'w') as f:
                    if instance.expect_json:
                        json.dump(result, f, indent=2)
                    else:
                        f.write(result)

        # Check if item is in the cache and call the decorated method if
        # needed
        do_cache = settings.CACHE_MIDDLEWARE and instance.cache_seconds
        if do_cache:
            classname = instance.__class__.__name__
            stringified_args = classname + " " + str(kwargs)
            key = hashlib.md5(stringified_args).hexdigest()
            result = get_cached_result(key, instance, stringified_args)
            if result is not None:
                return result

        # Didn't find it in the cache or not using a cache, so run our
        # function
        result = function(instance, *args, **kwargs)

        if do_cache:
            refresh_caches(key, instance, result)
        return result

    return memoizer


SERVICES_LIST = ('socorro.dataservice.services.bugs_service.BugsService',)

# Allow configman to dynamically load the configuration and classes
# for our API dataservice objects
def_source = Namespace()
def_source.namespace('services')
def_source.services.add_option(
    'service_list',
    doc='a list of classes that represent services to expose',
    default=','.join(SERVICES_LIST),
    from_string_converter=classes_in_namespaces_converter('service_class')
)

# setup configman to create all the configuration information for the
# dataservices classes.  Save that confguration in the key
# "DATASERVICE_CONFIG" within the settings imported from django.conf
settings.DATASERVICE_CONFIG = configuration(
    definition_source=[
        def_source,
        App.get_required_config(),
    ],
    values_source_list=[
        settings.DATASERVICE_CONFIG_BASE,
        # ConfigFileFutureProxy,  # for config files, not currently used
        environment
    ]
)

# we need to create model classes for each of the services in SERVICES_LIST.
# That list, however, may have been changed by configman during the
# initialization process.  So we iterate over the settings to create a model
# each of the services that configman has given in configuration.

# this mapping is keyed by the service class name, with values being the
# models representing those classes for Django.
service_class_name_to_model_class_mapping = {}

# settings.DATASERVICE_CONFIG came from confgiman. It defines a set of nested
# namespaces that define a dataservice.  Here, we loop over the configuration
# looking for namespaces - for each one found, we create a ModelForDataSrevice
# class for that service and save it.
for key in settings.DATASERVICE_CONFIG.keys_breadth_first(include_dicts=True):
    if (
        key.startswith('services')
        and '.' in key
        and isinstance(settings.DATASERVICE_CONFIG[key], DotDict)
    ):
        local_config = settings.DATASERVICE_CONFIG[key]
        impl_class = local_config.service_class

        # This class is the template for all the model classes that represent
        # the dataservice classes.  We populate it with class level attributes
        # and the appropriate methods.
        class ModelForDataService(ServiceBase):
            # implemetation_class will be a class from the
            # socorro/dataservice/services package. Classes from this location
            # are
            implementation_class = impl_class

            # in this section, we define the class attributes for a
            # dataservice by pulling values out of the configuration provided
            # by configman.
            required_params = local_config.required_params
            possible_params = local_config.possible_params
            expect_json = local_config.output_is_json
            cache_seconds = local_config.cache_seconds
            uri = local_config.uri
            is_documented = True

            API_BINARY_RESPONSE = local_config.api_binary_response
            API_BINARY_FILENAME = local_config.api_binary_filename
            API_BINARY_PERMISSIONS = local_config.api_binary_permissions
            API_WHITELIST = local_config.api_whitelist
            API_REQUIRED_PERMISSIONS = local_config.api_required_permissions

            def __init__(self, config=local_config):
                self.config = config

            # @memoize
            def get(self, **kwargs):
                params = external_common.parse_arguments(
                    self.filters,
                    kwargs
                )
                impl = self.implementation_class(local_config)
                try:
                    result = getattr(impl, local_config.method)(**kwargs)
                except Exception, x:
                    print 'get ^^^^^', x
                    raise
                print 'get *****', result
                return result

            # @memoize
            def post(self, **kwargs):
                params = external_common.parse_arguments(
                    self.filters,
                    kwargs
                )
                impl = self.implementation_class(local_config)
                try:
                    result = getattr(impl, local_config.method)(**kwargs)
                except Exception, x:
                    print 'put ^^^^^', x
                    raise
                print 'put *****', result
                return result

        # rename the template class with the same name as the dataservice
        # class
        ModelForDataService.__name__ = impl_class.__name__

        # save the newly created class to a mapping to preserve it.  This
        # allows the newly created class to survive outside of the loop.
        service_class_name_to_model_class_mapping[impl_class.__name__] = \
            ModelForDataService
