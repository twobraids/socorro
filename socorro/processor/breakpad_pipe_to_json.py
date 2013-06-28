# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from socorro.lib.util import DotDict

def _get(indexable_container, index, default):
    try:
        return indexable_container[index]
    except (IndexError, KeyError):
        return default

def _get_int(indexable_container, index, default):
    try:
        return int(indexable_container[index])
    except (IndexError, KeyError):
        return default
    except ValueError:  # separate case for future modifications
        return default

def _extract_OS_info(os_line, json_dump):
    system_info = DotDict()
    system_info.os = _get(os_line, 1, 'unknown')
    system_info.os_ver = _get(os_line, 2, 'unknown')
    if 'system_info' in json_dump:
        json_dump.system_info.update(system_info)
    else:
        json_dump.system_info = system_info

def _extract_CPU_info(cpu_line, json_dump):
    system_info = DotDict()
    system_info.cpu_arch = _get(cpu_line, 1, 'unknown')
    system_info.cpu_info = _get(cpu_line, 2, 'unknown')
    system_info.cpu_count = _get_int(cpu_line, 3, None)
    if 'system_info' in json_dump:
        json_dump.system_info.update(system_info)
    else:
        json_dump.system_info = system_info

def _extract_crash_info(crash_line, json_dump):
    crash_info = DotDict()
    crash_info.type = _get(crash_line, 1, 'unknown')
    crash_info.crash_address = _get(crash_line, 2, 'unknown')
    crash_info.crashing_thread = _get_int(crash_line, 3, None)
    json_dump.crash_info = crash_info
    return crash_info.crashing_thread

def _extract_module_info(module_line, json_dump, module_counter):
    module = DotDict()
    module.filename = _get(module_line, 1, 'unknown')
    if 'firefox' in module.filename:
        json_dump.main_module = module_counter
    module.version = _get(module_line, 2, 'unknown')
    module.debug_file = _get(module_line, 3, 'unknown')
    module.debug_id = _get(module_line, 4, 'unknown')
    module.base_addr = _get(module_line, 5, 'unknown')
    module.end_addr = _get(module_line, 6, 'unknown')
    if 'modules' not in json_dump:
        json_dump.modules = []
    json_dump.modules.append(module)

def _extract_frame_info(frame_line, json_dump):
    if 'threads' not in json_dump:
        json_dump.threads = []
    thread_number = _get_int(frame_line, 0, None)
    if thread_number is None:
        return
    if thread_number >=len(json_dump.threads):
        for i in range(thread_number - len(json_dump.threads) + 1):
            thread = DotDict()
            thread.frame_count = 0
            thread.frames = []
            json_dump.threads.append(thread)
    frame = DotDict()
    frame.frame = _get_int(frame_line, 1, None)
    frame.module = _get(frame_line, 2, 'unknown')
    frame.function = _get(frame_line, 3, 'unknown')
    frame.file = _get(frame_line, 4, 'unknown')
    frame.line = _get_int(frame_line, 5, None)
    frame.offset = _get(frame_line, 6, 'unknown')
    frame.module_offset = _get(frame_line, 7, 'unknown')
    frame.function_offset = _get(frame_line, 8, 'unknown')
    json_dump.threads[thread_number].frames.append(frame)
    json_dump.threads[thread_number].frame_count += 1


def pipe_dump_to_json_dump(pipe_dump_iterable):
    json_dump = DotDict()
    crashing_thread = None
    module_counter = 0
    thread_counter = 0
    for a_line in pipe_dump_iterable:
        parts = a_line.split('|')
        if parts[0] == 'OS':
            _extract_OS_info(parts, json_dump)
        elif parts[0] == 'CPU':
            _extract_CPU_info(parts, json_dump)
        elif parts[0] == 'Crash':
            crashing_thread = _extract_crash_info(parts, json_dump)
        elif parts[0] == 'Module':
            _extract_module_info(parts, json_dump, module_counter)
            module_counter += 1
        else:
            try:
                thread_number = int(parts[0])
            except (ValueError, IndexError):
                continue  # unknow line type, ignore it
            _extract_frame_info(parts, json_dump)
    json_dump.thread_count = len(json_dump.threads)
    if crashing_thread is not None:
        crashing_thread_frames = DotDict()
        crashing_thread_frames.threads_index = crashing_thread
        crashing_thread_frames.total_frames = \
            len(json_dump.threads[crashing_thread].frames)
        crashing_thread_frames.frames = \
            json_dump.threads[crashing_thread].frames[:10]
        json_dump.crashing_thread = crashing_thread_frames
    return json_dump
