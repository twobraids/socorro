# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""This package is a Socorro implementation of bsmedberg's skunk processor
crash classification system.  This package is intended for use in Socorro
Processor2012.  It is implemented on top of the Processor TransformRule
System.  It is intended that this package demonstrate the implementation
technique for all future enhancements to Processor2012 and well as how
the entirety of the future BixieProcessor2013 and SocorroProcessor2014
implementations.

The intent is to provide a framework for which ad hoc classification rules
can be added to the processor easily.  The rules, that all follow the same
programatic form, are executed one at a time, in order, until one of the rules
reports that it succeeds.  At that point, execution of classificiation rules
stops.

Classification rules are applied after all other processing of a crash is
complete.  Classification rules are given access to both the raw_crash and the
processed_crash.  In this version, the classification rule has complete read
and write access to all fields with in both versions two versions of the crash.
Rules should probably refrain from making modifications and instead use the
base class API for adding a classification to the processed_cash.

All rules have two major components: a predicate and an action.

The predicate: this method is used to initially determine if the rule should
be applied.

The action: if the predicate succeeds, the 'action' function is executed.  If
this method succeeds, it uses the '_add_classification' method from the base
class to tag the crash with new data and return True.  If this method fails or
otherwise decides that its action should not apply, it just quits returning
False.  The act of returning False tells the underlying TransformRule system to
continue trying to apply rules.

Successful application of an action results in the following structure to be
added to the processed crash:

{...
    'classifications': {
        'skunk_works': {
            'classification': 'some classification',
            'classification_data': 'extra information saved by rule',
            'classificaiton_version': '0.0',
        }
    }

...}
"""

from socorro.lib.util import DotDict


#==============================================================================
class SkunkClassificationRule(object):
    """the base class for Skunk Rules.  It provides the framework for the rules
    'predicate', 'action', and 'version' as well as utilites to help rules do
    their jobs."""

    #--------------------------------------------------------------------------
    def predicate(self, raw_crash,  processed_crash, processor):
        """"The default predicate is too look into the processed crash to see
        if the 'skunk_works' classification has already been applied.
        parameters:
            raw_crash - a mapping representing the raw crash data originally
                        submitted by the client
            processed_crash - the ultimate result of the processor, this is the
                              analized version of a crash.  It contains the
                              output of the MDSW program for each of the dumps
                              within the crash.
            processor - a reference to the processor object that is assigned
                        to working on the current crash. This object contains
                        resources that might be useful to a classifier rule.
                        'processor.config' is the configuration for the
                        processor in which database connection paramaters can
                        be found.  'processor.logger' is useful for any logging
                        of debug information. 'processor.c_signature_tool' or
                        'processor.java_signature_tool' contain utilities that
                        might be useful during classification.

        returns:
            True - this rule should be applied
            False - this rule should not be applied
        """
        try:
            if 'classification' in processed_crash.classifications.skunk_works:
                return False
            return True
        except KeyError:
            return True

    #--------------------------------------------------------------------------
    def action(self, raw_crash,  processed_crash, processor):
        """Rules derived from this base class ought to override this method
        with an actual classification rule.  Successful application of this
        method should include a call to '_add_classification'.

        parameters:
            raw_crash - a mapping representing the raw crash data originally
                        submitted by the client
            processed_crash - the ultimate result of the processor, this is the
                              analized version of a crash.  It contains the
                              output of the MDSW program for each of the dumps
                              within the crash.
            processor - a reference to the processor object that is assigned
                        to working on the current crash. This object contains
                        resources that might be useful to a classifier rule.
                        'processor.config' is the configuration for the
                        processor in which database connection paramaters can
                        be found.  'processor.logger' is useful for any logging
                        of debug information. 'processor.c_signature_tool' or
                        'processor.java_signature_tool' contain utilities that
                        might be useful during classification.

        returns:
            True - this rule was applied successfully and no further rules
                   should be applied
            False - this rule did not succeed and further rules should be
                    tried
        """
        return True

    #--------------------------------------------------------------------------
    def version(self):
        """This method should be overridden in a base class."""
        return '0.0'

    #--------------------------------------------------------------------------
    def _add_classification(
        self,
        processed_crash,
        classification,
        classification_data
    ):
        """This method adds a 'skunk_works' classification to a processed
        crash.

        parameters:
            processed_crash - a reference to the processed crash to which the
                              classification is to be added.
            classification - a string that is the classification.
            classification_data - a string of extra data that goes along with a
                                  classification
        """
        if 'classifications' not in processed_crash:
            processed_crash.classifications = DotDict()
        processed_crash.classifications['skunk_works'] = DotDict({
            'classification': classification,
            'classification_data': classification_data,
            'classification_version': self.version()
        })

    #--------------------------------------------------------------------------
    @staticmethod
    def _get_stack(processed_crash, dump_name='plugin'):
        """This utility method offers derived classes a way to fetch the stack
        for the thread that caused the crash

        parameters:
            processed_crash - this is the mapping that contains the MDSW output
            dump_name - each dump within a crash has a name.

        returns:
            False - if something goes wrong in trying to get the stack
            a list of frames from the crashing thread of the specified dump in
            this form:
                [
                    {
                        'module': '...',
                        'function': '...',
                        'file': '...',
                        'line': '...',
                        'offset': '...',
                        'module_offset': '...',
                        'funtion_offset': '...',
                    }, ...
                ]

        """
        try:
            a_json_dump = processed_crash[dump_name].json_dump
        except KeyError:
            # no plugin or plugin json dump
            return False
        try:
            stack = \
                a_json_dump['threads'][
                    a_json_dump['crash_info']['crashing_thread']
                ]['frames']
        # these exceptions are kept as separate cases just to help keep track
        # of what situations they cover
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
        """this utility will return a boolean indicitating if a string
        appears at the beginning of any of the nomalized frame signatures
        within a stack.

        parameters:
            stack - the stack to examine
            signature - the string to search for
            a_signature_tool - a reference to an object having a
                               'normalize_signature' method.  This is applied
                               to each frame of the stack prior to testing
            cache_normalizations - sometimes many rules will need to do the
                                   same normalizations over and over.  This
                                   method will save the normalized form of a
                                   stack frame within the processed_crash's
                                   copy of the processed stack.  This cache
                                   will persist and get saved to processed
                                   crash storage.
        """
        for a_frame in stack:
            try:
                normalized_frame = a_frame['normalized']
            except KeyError:
                normalized_frame = a_signature_tool.normalize_signature(
                    **a_frame
                )
                if cache_normalizations:
                    a_frame['normalized'] = normalized_frame
            if normalized_frame.startswith(signature):
                return True
        return False


#==============================================================================
class UpdateWindowAttributes(SkunkClassificationRule):
    """"""
    #--------------------------------------------------------------------------
    def version(self):
        return '0.1'

    #--------------------------------------------------------------------------
    def action(self, raw_crash,  processed_crash, processor):
        stack = self._get_stack(processed_crash, 'plugin')
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

        current_target_signature = target_signatures.pop(0)
        for i, a_frame in enumerate(stack):
            normalized_signature = \
                processor.c_signature_tool.normalize_signature(**a_frame)
            if (current_target_signature in normalized_signature):
                current_target_signature = target_signatures.pop(0)
                if not target_signatures:
                    break
        if target_signatures:
            return False

        try:
            classification_data = \
                processor.c_signature_tool.normalize_signature(**stack[i + 1])
        except IndexError:
            classification_data = None

        self._add_classification(
            processed_crash,
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
    def action(self, raw_crash,  processed_crash, processor):
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
                            'NtUserSetWindowPos | %s' % a_second_sentinel,
                            None
                        )
                        return True
            self._add_classification(
                processed_crash,
                'NtUserSetWindowPos | other',
                None
            )
            return True
        return False


#==============================================================================
class SendWaitReceivePort(SkunkClassificationRule):
    #--------------------------------------------------------------------------
    def version(self):
        return '0.1'

    #--------------------------------------------------------------------------
    def action(self, raw_crash,  processed_crash, processor):

        stack = self._get_stack(processed_crash, 'flash2')
        if stack is False:
            return False

        if self._stack_contains(
            stack[:5],
            'NtAlpcSendWaitReceivePort',
            processor.c_signature_tool,
        ):
            self._add_classification(
                processed_crash,
                'NtAlpcSendWaitReceivePort',
                None
            )

            return True
        return False


#==============================================================================
class Bug811804(SkunkClassificationRule):
    #--------------------------------------------------------------------------
    def version(self):
        return '0.1'

    #--------------------------------------------------------------------------
    def action(self, raw_crash,  processed_crash, processor):

        try:
            signature = processed_crash.flash2.signature
        except KeyError:
            return False

        if (signature == 'hang | NtUserWaitMessage | F34033164'
                         '________________________________'):
            self._add_classification(
                processed_crash,
                'bug811804-NtUserWaitMessage',
                None
            )
            return True
        return False


#==============================================================================
class Bug812318(SkunkClassificationRule):
    #--------------------------------------------------------------------------
    def version(self):
        return '0.1'

    #--------------------------------------------------------------------------
    def action(self, raw_crash,  processed_crash, processor):
        stack = self._get_stack(processed_crash, 'flash2')
        if stack is False:
            return False

        primary_found = self._stack_contains(
            stack[:5],
            'NtUserPeekMessage',
            processor.c_signature_tool,
        )
        if not primary_found:
            return False

        secondary_found = self._stack_contains(
            stack[:5],
            'F849276792______________________________',
            processor.c_signature_tool,
        )
        if secondary_found:
            classification = 'bug812318-PeekMessage'
        else:
            classification = 'NtUserPeekMessage-other'
        self._add_classification(
            processed_crash,
            classification,
            None
        )
        return True
