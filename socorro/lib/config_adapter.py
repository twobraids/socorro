from socorro.lib.ConfigurationManager import Option

from configman.converters import class_converter
from configman.dotdict import DotDict

mappings = {'batchJobLimit': 'batch_job_limit',
            'checkForPriorityFrequency': 'check_for_priority_frequency',
            'collectAddon': 'collect_addons',
            'collectCrashProcess': 'collect_crash_process',
            'crashingThreadFrameThreshold': 'crashing_thread_frame_threshold',
            'crashingThreadTailFrameThreshold': 'crashing_thread_tail_frame_threshold',
            'databaseHost': 'database_host',
            'databaseName': 'database_name'}
            

def config_adapter(old_module_as_string):
    m = class_converter(old_module_as_string)
    symbols = [x for x in dir(m)]
    adapted_values = {}
    for a_symbol in symbols:
        a_value = getattr(m, a_symbol)
        if isinstance(a_value, Option):
            try:
                adapted_values[mappings[a_symbol]] = getattr(m, a_symbol).default
            except KeyError:
                pass
    return adapted_values






