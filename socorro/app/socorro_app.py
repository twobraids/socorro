#! /usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""This module defines the class hierarchy for all Socorro applications.

The base of the hierarchy is "SocorroApp" which defines the interface and some
of the base methods.

Derived from the base "SocorroApp" is the "App" class.  This class adds logging
configuration requirements to the application.  App is the class from which
all the Socorro Apps derive.

Also derived from the base class "SocorroApp" is "SocorroWelcomeApp", an app
that serves as a dispatcher for all other Socorro apps.  Rather than forcing
the user to know what and where all the other Socorro apps are, this app adds
an "application" config requirement as a commandline arguement.  The user may
specify the app name that they want to run.  Running --help on this app, will
also list all the Socorro Apps.

If a configuration file exists that includes a not-commented-out 'application'
parameter, it can be give directly to the "SocorroWelomeApp".  In that case,
the "SocorroWelcomeApp" becomes the app requested in the config file.
"""

import logging
import logging.handlers
import functools
import signal
import os
import re
import threading

import socorro.app.for_application_defaults
from socorro.app.for_application_defaults import (
    ApplicationDefaultsProxy,
)

from configman import (
    ConfigurationManager,
    Namespace,
    RequiredConfig,
    ConfigFileFutureProxy,
    environment,
    command_line,
)
from configman.converters import str_to_python_object, py_obj_to_str

#------------------------------------------------------------------------------
# every socorro app has a class method called 'get_application_defaults' from
# which configman extracts the preferred configuration default values.
#
# The Socorro App class hierachy will create a 'values_source_list' with the
# App's preferred config at the base.  These become the defaults over which
# the configuration values from config file, environment, and command line are
# overlaid.
#
# In the case where the actual app is not specified until configman is already
# invoked, application defaults cannot be determined until configman
# has already started the overlay process.  To resolve this
# chicken/egg problem, we create a ApplicationDefaultsProxy class that stands
# in values_source list (the list of places that overlay config values come
# from).  Since the ApplicationDefaultsProxy also serves as the 'from_string'
# converter for the Application config option, it can know when the target
# application has been determined, fetch the defaults.  Since the
# ApplicationDefaultsProxy object is already in the values source list, it can
# then start providing overlay values immediately.

# Configman knows nothing about how the ApplicationDefaultsProxy object works,
# so we must regisiter it as a new values overlay source class.  We do that
# by manually inserting inserting the new class into Configman's
# handler/dispatcher.  That object associates config sources with modules that
# are able to implement Configman's overlay handlers.
from configman.value_sources import type_handler_dispatch
# register our new type handler with configman
type_handler_dispatch[ApplicationDefaultsProxy].append(
    socorro.app.for_application_defaults
)

#------------------------------------------------------------------------------
# create the app default proxy object
application_defaults_proxy = ApplicationDefaultsProxy()

#------------------------------------------------------------------------------
# for use with SIGHUP for apps that run as daemons
restart = True


#------------------------------------------------------------------------------
def respond_to_SIGHUP(signal_number, frame, logger=None):
    """raise the KeyboardInterrupt which will cause the app to effectively
    shutdown, closing all it resources.  Then, because it sets 'restart' to
    True, the app will reread all the configuration information, rebuild all
    of its structures and resources and start running again"""
    global restart
    restart = True
    if logger:
        logger.info('detected SIGHUP')
    raise KeyboardInterrupt


#==============================================================================
class SocorroApp(RequiredConfig):
    """The base class for all Socorro applications"""
    app_name = 'SocorroAppBaseClass'
    app_version = "1.0"
    app_description = 'base class for app system'

    required_config = Namespace()

    #--------------------------------------------------------------------------
    def __init__(self, config):
        self.config = config

    #--------------------------------------------------------------------------
    @staticmethod
    def get_application_defaults():
        """this method allows an app to inject defaults into the configuration
        that can override defaults not under the direct control of the app.
        For example, if an app were to use a class that had a config default
        of X and that was not appropriate as a default for this app, then
        this method could be used to override that default"""
        return {}

    #--------------------------------------------------------------------------
    def main(self):  # pragma: no cover
        """derived classes must override this function with business logic"""
        raise NotImplementedError(
            "A definition of 'main' in a derived class is required"
        )

    #--------------------------------------------------------------------------
    @classmethod
    def run(klass):
        global restart
        restart = True
        while restart:
            # the SIGHUP handler will change that back to True if it wants
            # the app to restart and run again.
            restart = False
            app_exit_code = klass._do_run()
        return app_exit_code

    #--------------------------------------------------------------------------
    @classmethod
    def _do_run(klass):
        # while this method is defined here, only derived classes are allowed
        # to call it.
        if klass is SocorroApp:
            raise NotImplementedError(
                "The SocorroApp class has no useable 'main' method"
            )

        config_path = os.environ.get(
            'DEFAULT_SOCORRO_CONFIG_PATH',
            './config'
        )

        values_source_list = [
            application_defaults_proxy,
            ConfigFileFutureProxy,
            environment,
            command_line
        ]

        config_definition = klass.get_required_config()
        if 'application' not in config_definition:
            application_config = Namespace()
            application_config.add_option(
                'application',
                is_argument=True,
                doc=(
                    'the fully qualified classname of the app to run'
                ),
                default=py_obj_to_str(klass),
                from_string_converter=(
                    application_defaults_proxy.str_to_application_class
                ),
            )
            config_definition = application_config

        config_manager = ConfigurationManager(
            config_definition,
            app_name=klass.app_name,
            app_version=klass.app_version,
            app_description=klass.app_description,
            values_source_list=values_source_list,
            options_banned_from_help=[],
            config_pathname=config_path
        )

        def fix_exit_code(code):
            # some apps don't return a code so you might get None
            # which isn't good enough to send to sys.exit()
            if code is None:
                return 0
            return code

        with config_manager.context() as config:
            config.executor_identity = (
                lambda: threading.currentThread().getName()
            )
            try:
                config_manager.log_config(config.logger)
                respond_to_SIGHUP_with_logging = functools.partial(
                    respond_to_SIGHUP,
                    logger=config.logger
                )
                # install the signal handler with logging
                signal.signal(signal.SIGHUP, respond_to_SIGHUP_with_logging)
            except KeyError:
                # install the signal handler without logging
                signal.signal(signal.SIGHUP, respond_to_SIGHUP)

            app_to_run = klass(config)
            app_to_run.config_manager = config_manager
            # whew, finally run the app that we wanted

            return_code = fix_exit_code(app_to_run.main())
            return return_code


#==============================================================================
class LoggerWrapper(object):
    """This class wraps the standard logger object.  It changes the logged
    messages to display the 'executor_identity': the thread/greenlet/process
    that is currently running."""

    #--------------------------------------------------------------------------
    def __init__(self, logger, config):
        self.config = config
        self.logger = logger

    #--------------------------------------------------------------------------
    def executor_identity(self):
        try:
            return " - %s - " % self.config.executor_identity()
        except KeyError:
            return " - %s - " % threading.currentThread().getName()

    #--------------------------------------------------------------------------
    def debug(self, message, *args, **kwargs):
        self.logger.debug(self.executor_identity() + message, *args, **kwargs)

    #--------------------------------------------------------------------------
    def info(self, message, *args, **kwargs):
        self.logger.info(self.executor_identity() + message, *args, **kwargs)

    #--------------------------------------------------------------------------
    def error(self, message, *args, **kwargs):
        self.logger.error(self.executor_identity() + message, *args, **kwargs)

    #--------------------------------------------------------------------------
    def warning(self, message, *args, **kwargs):
        self.logger.warning(
            self.executor_identity() + message,
            *args,
            **kwargs
        )

    #--------------------------------------------------------------------------
    def critical(self, message, *args, **kwargs):
        self.logger.critical(
            self.executor_identity() + message,
            *args,
            **kwargs
        )


#------------------------------------------------------------------------------
def setup_logger(config, local_unused, args_unused):
    """This method is sets up and initializes the logger objects.  It is a
    function in the form appropriate for a configiman aggregation.  When given
    to Configman, that library will setup and initialize the logging system
    automatically and then offer the logger as an object within the
    configuration object."""
    app_name = config.application.app_name
    logger = logging.getLogger(app_name)
    # if this is a restart, loggers must be removed before being recreated
    tear_down_logger(app_name)
    logger.setLevel(logging.DEBUG)
    stderr_log = logging.StreamHandler()
    stderr_log.setLevel(config.logging.stderr_error_logging_level)
    stderr_format = config.logging.stderr_line_format_string.replace(
        '{app_name}',
        app_name
    )
    stderr_log_formatter = logging.Formatter(
        _convert_format_string(stderr_format)
    )
    stderr_log.setFormatter(stderr_log_formatter)
    logger.addHandler(stderr_log)

    syslog = logging.handlers.SysLogHandler(
        facility=config.logging.syslog_facility_string
    )
    syslog.setLevel(config.logging.syslog_error_logging_level)
    syslog_format = config.logging.syslog_line_format_string.replace(
        '{app_name}',
        app_name
    )
    syslog_formatter = logging.Formatter(
        _convert_format_string(syslog_format)
    )
    syslog.setFormatter(syslog_formatter)
    logger.addHandler(syslog)

    wrapped_logger = LoggerWrapper(logger, config)
    return wrapped_logger


#==============================================================================
class App(SocorroApp):
    """The base class from which Socorro apps are based"""
    required_config = Namespace()
    required_config.namespace('logging')
    required_config.logging.add_option(
        'syslog_host',
        doc='syslog hostname',
        default='localhost',
        reference_value_from='resource.logging',
    )
    required_config.logging.add_option(
        'syslog_port',
        doc='syslog port',
        default=514,
        reference_value_from='resource.logging',
    )
    required_config.logging.add_option(
        'syslog_facility_string',
        doc='syslog facility string ("user", "local0", etc)',
        default='user',
        reference_value_from='resource.logging',
    )
    required_config.logging.add_option(
        'syslog_line_format_string',
        doc='python logging system format for syslog entries',
        default='{app_name} (pid {process}): '
                '{asctime} {levelname} - {threadName} - '
                '{message}',
        reference_value_from='resource.logging',
    )
    required_config.logging.add_option(
        'syslog_error_logging_level',
        doc='logging level for the log file (10 - DEBUG, 20 '
            '- INFO, 30 - WARNING, 40 - ERROR, 50 - CRITICAL)',
        default=40,
        reference_value_from='resource.logging',
    )
    required_config.logging.add_option(
        'stderr_line_format_string',
        doc='python logging system format for logging to stderr',
        default='{asctime} {levelname} - {app_name}.{threadName} - '
                '{message}',
        reference_value_from='resource.logging',
    )
    required_config.logging.add_option(
        'stderr_error_logging_level',
        doc='logging level for the logging to stderr (10 - '
            'DEBUG, 20 - INFO, 30 - WARNING, 40 - ERROR, '
            '50 - CRITICAL)',
        default=10,
        reference_value_from='resource.logging',
    )

    required_config.add_aggregation(
        'logger',
        setup_logger
    )


#==============================================================================
class SocorroWelcomeApp(SocorroApp):
    required_config = Namespace()
    required_config.add_option(
        'application',
        is_argument=True,
        doc=(
            'the name of the app to run (select from: %s)' %
            ', '.join(sorted(application_defaults_proxy.apps.keys()))
        ),
        default=None,
        from_string_converter=(
            application_defaults_proxy.str_to_application_class
        ),
    )
    app_name = "SocorroWelcomeApp"
    app_version = "1.0"
    app_description = 'Welcome to Socorro'

    #--------------------------------------------------------------------------
    def main(self):
        if (
            self.config.application
            and self.config.application.__name__ is not self.__class__.__name__
        ):
            # specifically targeted at CronTabber a foreign app that doesn't
            # derive from the SocorroApp hierarchy.
            if not issubclass(self.config.application, SocorroApp):
                class_path = py_obj_to_str(self.config.application)
                module_path = '.'.join(class_path.split('.')[:-1])
                main_path = module_path + ".main"
                foreign_main = str_to_python_object(main_path)
                return foreign_main(self.config.application)

            requested_app = self.config.application(self.config)
            # this is where an app that was requested through the use of the
            # config parameter 'application' is actually run
            requested_app.config_manager = self.config_manager
            return requested_app.main()
        else:
            print (
                "Welcome to Socorro.  To configure Socorro, please see "
                "http://socorro.readthedocs.org/en/latest/\n\n"
                "use --help with this app to see what you can do here"
            )


#------------------------------------------------------------------------------
def tear_down_logger(app_name):
    logger = logging.getLogger(app_name)
    # must have a copy of the handlers list since we cannot modify the original
    # list while we're deleting items from that list
    handlers = [x for x in logger.handlers]
    for x in handlers:
        logger.removeHandler(x)


#------------------------------------------------------------------------------
def _convert_format_string(s):
    """return '%(foo)s %(bar)s' if the input is '{foo} {bar}'"""
    return re.sub('{(\w+)}', r'%(\1)s', s)


#------------------------------------------------------------------------------
def main(app_class):
    return app_class.run()

if __name__ == '__main__':
    main(SocorroWelcomeApp)
