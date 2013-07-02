from socorro.lib.util import DotDict
"""

"""


#==============================================================================
# Predicates

#------------------------------------------------------------------------------
def always_true_predicate(raw_crash, processed_crash, processor):
    return True

#------------------------------------------------------------------------------
def classification_already_set_predicate(
    raw_crash,
    processed_crash,
    processor
):
    try:
        if 'classification' in processed_crash.classifications.skunk_works:
            return False
        return True
    except KeyError:
        return True


#==============================================================================
# Utilities

#------------------------------------------------------------------------------
def _add_classification(
    processed_crash,
    category,
    classification,
    classification_data
):
    if 'classifications' not in processed_crash:
        processed_crash.classifications = DotDict()
    processed_crash.classifications[category] = DotDict({
        'classification': classification,
        'classification_data': classification_data
    })


#------------------------------------------------------------------------------
def _get_stack(processed_crash, dump_name='plugin'):
    try:
        a_json_dump = processed_crash[dump_name].json_dump
    except KeyError:
        # no plugin or plugin json dump
        return False
    try:
        stack = a_json_dump.threads[a_json_dump.crash_info.crashing_thread]
    except KeyError:
        # no threads or no crash_info or no crashing_thread
        return False
    except IndexError:
        # no stack for crashing_thread
        return False
    except ValueError:
        # crashing_thread is not an integer
        return False
    return stack


#==============================================================================
# Rules

#------------------------------------------------------------------------------
def update_window_attributes_classifier(raw_crash, processed_crash, processor):
    stack = _get_stack(processed_crash)
    if stack is False:
        return False

    # normalize the stack of the crashing thread and find the 3 target frames
    target_signatures = [
        "F_1152915508___________________________________",
        "mozilla::plugins::PluginInstanceChild::UpdateWindowAttributes(bool)",
        "mozilla::ipc::RPCChannel::Call(IPC::Message*, IPC::Message*)"
    ]

    current_target_signature = target_signatures.pop()
    for i, a_frame in enumerate(stack):
        if (processor.c_signature_tool.normalize_signature(**a_frame) ==
            current_target_signature):
            current_target_signature = target_signatures.pop()
            if not target_signatures:
                break
    if target_signatures:
        return False

    try:
        classification_data = processor.c_signature_tool.normalize_signature(
            stack[i + 1]
        )
    except IndexError:
        classification_data = None

    _add_classification(
        processed_crash,
        'skunk_works',
        'adbe-3355131',
        classification_data
    )

    return True


#------------------------------------------------------------------------------
def classify_set_window_pos(raw_crash, processed_crash, processor):
    found = _do_set_window_pos_classification(
        processed_crash,
        'plugin',
        ('F_320940052', 'F_1378698112', 'F_468782153')
    )
    if not found:
        found = _do_set_window_pos_classification(
            processed_crash,
            'flash2',
            ('F455544145',)
        )
    return found

#------------------------------------------------------------------------------
def _do_set_window_pos_classification(
    processed_crash,
    dump_name,
    secondary_sentinels
):
    truncated_stack = _get_stack(processed_crash, dump_name)[:5]
    stack_contains_sentinel = reduce(
        lambda x, y: x or y.startswith('NtUserSetWindowPos'),
        truncated_stack,
        False
    )
    if stack_contains_sentinel:
        for a_second_sentinel in secondary_sentinels:
            stack_contains_secondary = reduce(
                lambda x, y: x or y.startswith(a_second_sentinel),
                truncated_stack,
                False
            )
            if stack_contains_secondary:
                    _add_classification(
                        processed_crash,
                        'skunk_works',
                        'NtUserSetWindowPos | %s' % a_second_sentinel,
                        None
                    )
                    return True
        _add_classification(
            processed_crash,
            'skunk_works',
            'NtUserSetWindowPos | other',
            None
        )
        return True
    return False





