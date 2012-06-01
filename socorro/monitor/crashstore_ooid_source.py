from configman import Namespace, RequiredConfig
from configman.converters import class_converter

#==============================================================================
class CrashStorageOoidSource(RequiredConfig):
    """this class provides an iterator that will pull from any crash
    storage class new_ooids generator"""
    required_config = Namespace()
    required_config.add_option(
        'crashstorage_class',
        doc="the class of the crashstorage system",
        default='socorro.external.filesystem.crashstorage.'
                'FileSystemRawCrashStorage',
        from_string_converter=class_converter
    )

    def __init__(self, config, quit_check_callback=None):
        self.config = config
        self.crash_store = config.crashstorage_class(config)

    def __call__(self):
        return self.crash_store.new_ooids()

    def close(self):
        self.crash_store.close()
