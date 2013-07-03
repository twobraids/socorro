# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from socorro.lib.transform_rules import TransformRule


#==============================================================================
class SkunkClassificationRule(TransformRule):

    #--------------------------------------------------------------------------
    def predicate(self, raw_crash,  processed_crash, processor):
        try:
            if 'classification' in processed_crash.classifications.skunk_works:
                return False
            return True
        except KeyError:
            return True

    #--------------------------------------------------------------------------
    def action(self, raw_crash,  processed_crash, processor):
        return False

    #--------------------------------------------------------------------------
    def version(self):
        return '0.0'

    #--------------------------------------------------------------------------
    def _add_classification_to_processed_crash(
        self,
        processed_crash,
        category,
        classification,
        classification_data
    ):
        if 'classifications' not in processed_crash:
            processed_crash.classifications = DotDict()
        processed_crash.classifications[category] = DotDict({
            'classification': classification,
            'classification_data': classification_data,
            'classification_version': self.version()
        })

    #--------------------------------------------------------------------------
    @staticmethod
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

    #--------------------------------------------------------------------------
    @staticmethod
    def _stack_contains(
        stack,
        signature,
        a_signature_tool,
        cache_normalizations=True
    ):
        for a_frame in stack:
            try:
                normalized_frame = a_frame['normalized']
            except KeyError:
                normalized_frame = a_signature_tool.normalize(**a_frame)
                if cache_normalizations:
                    a_frame['normalized'] = normalized_frame
            if normalized_frame.startswith(signature):
                return True
        return False

#==============================================================================
class UpdateWindowAttributes(SkunkClassificationRule):
    #--------------------------------------------------------------------------
    def version(self):
        return '0.1'

    #--------------------------------------------------------------------------
    def action(raw_crash,  processed_crash, processor):
        stack = _get_stack(processed_crash)
        if stack is False:
            return False

        # normalize the stack of the crashing thread
        # then look for these 3 target frames
        target_signatures = [
            "F_1152915508___________________________________",
            "mozilla::plugins::PluginInstanceChild::UpdateWindowAttributes"
                "(bool)",
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
            classification_data = \
                processor.c_signature_tool.normalize_signature(stack[i + 1])
        except IndexError:
            classification_data = None

        _add_classification(
            processed_crash,
            'skunk_works',
            'adbe-3355131',
            classification_data
        )

        return True


#==============================================================================
class SetWindowPos(SkunkClassificationRule):
    #--------------------------------------------------------------------------
    def version(self):
        return '0.1'

    #--------------------------------------------------------------------------
    def action(raw_crash,  processed_crash, processor):
        found = self._do_set_window_pos_classification(
            processed_crash,
            processor.c_signature_tool,
            'plugin',
            ('F_320940052', 'F_1378698112', 'F_468782153')
        )
        if not found:
            found = self._do_set_window_pos_classification(
                processed_crash,
                processor.c_signature_tool,
                'flash2',
                ('F455544145',)
            )
        return found

    #--------------------------------------------------------------------------
    def _do_set_window_pos_classification(
        self,
        processed_crash,
        signature_normalization_tool,
        dump_name,
        secondary_sentinels
    ):
        truncated_stack = self._get_stack(processed_crash, dump_name)[:5]
        stack_contains_sentinel = self._stack_contains(
            truncated_stack,
            'NtUserSetWindowPos',
            signature_normalization_tool,
        )
        if stack_contains_sentinel:
            for a_second_sentinel in secondary_sentinels:
                stack_contains_secondary = self._stack_contains(
                    truncated_stack,
                    a_second_sentinel,
                    signature_normalization_tool,
                )
                if stack_contains_secondary:
                        self._add_classification(
                            processed_crash,
                            'skunk_works',
                            'NtUserSetWindowPos | %s' % a_second_sentinel,
                            None
                        )
                        return True
            self._add_classification(
                processed_crash,
                'skunk_works',
                'NtUserSetWindowPos | other',
                None
            )
            return True
        return False
