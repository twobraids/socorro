# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import unittest
import copy
import datetime

from socorro.lib.util import DotDict, FakeLogger, SilentFakeLogger
from socorro.processor.skunk_classifiers import (
    SkunkClassificationRule,
    DontConsiderTheseFilter,
    UpdateWindowAttributes,
    SetWindowPos,
    SendWaitReceivePort,
    Bug811804,
    Bug812318,
)
from socorro.processor.signature_utilities import CSignatureTool
from socorro.unittest.processor.test_breakpad_pipe_to_json import (
    cannonical_json_dump
)

csig_config = DotDict()
csig_config.irrelevant_signature_re = ''
csig_config.prefix_signature_re = ''
csig_config.signatures_with_line_numbers_re = ''
c_signature_tool = CSignatureTool(csig_config)


class TestSkunkClassificationRule(unittest.TestCase):

    def test_predicate(self):
        rc = DotDict()
        pc = DotDict()
        pc.classifications = DotDict()
        processor = None

        skunk_rule = SkunkClassificationRule()
        self.assertTrue(skunk_rule.predicate(rc, pc, processor))

        pc.classifications.skunk_works = DotDict()
        self.assertTrue(skunk_rule.predicate(rc, pc, processor))

        pc.classifications.skunk_works.classification = 'stupid'
        self.assertFalse(skunk_rule.predicate(rc, pc, processor))

    def test_action(self):
        rc = DotDict()
        pc = DotDict()
        processor = None

        skunk_rule = SkunkClassificationRule()
        self.assertTrue(skunk_rule.action(rc, pc, processor))

    def test_version(self):
        skunk_rule = SkunkClassificationRule()
        self.assertEqual(skunk_rule.version(), '0.0')

    def test_add_classification_to_processed_crash(self):
        rc = DotDict()
        pc = DotDict()
        pc.classifications = DotDict()
        processor = None

        skunk_rule = SkunkClassificationRule()
        skunk_rule._add_classification(
            pc,
            'stupid',
            'extra stuff'
        )
        self.assertTrue('classifications' in pc)
        self.assertTrue('skunk_works' in pc.classifications)
        self.assertEqual(
            'stupid',
            pc.classifications.skunk_works.classification
        )
        self.assertEqual(
            'extra stuff',
            pc.classifications.skunk_works.classification_data
        )
        self.assertEqual(
            '0.0',
            pc.classifications.skunk_works.classification_version
        )

    def test_get_stack(self):
        pc = DotDict()
        skunk_rule = SkunkClassificationRule()
        processor = DotDict()

        self.assertFalse(skunk_rule._get_stack(pc, 'plugin'))

        pc.plugin = DotDict()
        pc.plugin.json_dump = DotDict()
        pc.plugin.json_dump.threads = []
        self.assertFalse(skunk_rule._get_stack(pc, 'plugin'))

        pc.plugin.json_dump.crash_info = DotDict()
        pc.plugin.json_dump.crash_info.crashing_thread = 1
        self.assertFalse(skunk_rule._get_stack(pc, 'plugin'))

        pc.plugin.json_dump = cannonical_json_dump
        self.assertEqual(
            skunk_rule._get_stack(pc, 'plugin'),
            cannonical_json_dump['threads'][0]['frames']
        )

    def test_stack_contains(self):
        stack = cannonical_json_dump['threads'][1]['frames']

        skunk_rule = SkunkClassificationRule()
        self.assertTrue(
            skunk_rule._stack_contains(
                stack,
                'ha_',
                c_signature_tool,
                cache_normalizations=False
            ),
        )
        self.assertFalse(
            skunk_rule._stack_contains(
                stack,
                'heh_',
                c_signature_tool,
                cache_normalizations=False
            ),
        )
        self.assertFalse('normalized' in stack[0])
        self.assertTrue(
            skunk_rule._stack_contains(
                stack,
                'ha_ha2',
                c_signature_tool,
            ),
        )
        self.assertTrue('normalized' in stack[0])


class TestDontConsiderTheseFilter(unittest.TestCase):

    def test_action_predicate_accept(self):
        """test all of the case where the predicate should return True"""
        filter_rule = DontConsiderTheseFilter()

        fake_processor = DotDict()
        fake_processor.config = DotDict()
        # need help figuring out failures? switch to FakeLogger and read stdout
        fake_processor.config.logger = SilentFakeLogger()
        #fake_processor.config.logger = FakeLogger()

        # find non-plugin crashes
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'browser'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find non-Firefox crashes
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Internet Explorer"
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with no Version info
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with faulty Version info
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = 'dwight'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with no BuildID info
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '17.1'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with faulty BuildID info
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '17.1'
        test_raw_crash.BuildID = '201307E2'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with faulty BuildID info (not integer)
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '17.1'
        test_raw_crash.BuildID = '201307E2'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with faulty BuildID info (bad month & day)
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '17.1'
        test_raw_crash.BuildID = '20131458'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with pre-17 version
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '15'
        test_raw_crash.BuildID = '20121015'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with 18 version but build date less than 2012-10-23
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '18'
        test_raw_crash.BuildID = '20121015'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with build date less than 2012-10-17
        # and version 17 or above
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '17'
        test_raw_crash.BuildID = '20121015'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            DotDict(),
            fake_processor
        ))

        # find crashes with no default dump
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # find crashes with no architecture info
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # find crashes with amd64 architecture info
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        test_processed_crash.json_dump.cpu_arch = 'amd64'
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # find crashes with main dump processing errors
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        test_processed_crash.json_dump.cpu_arch = 'x86'
        test_processed_crash.success = False
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # find crashes with extra dump processing errors
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        test_processed_crash.json_dump.cpu_arch = 'x86'
        test_processed_crash.success = True
        test_processed_crash.additional_minidumps = ['a', 'b', 'c']
        test_processed_crash.a = DotDict()
        test_processed_crash.a.success = True
        test_processed_crash.b = DotDict()
        test_processed_crash.b.success = True
        test_processed_crash.c = DotDict()
        test_processed_crash.c.success = False
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # find crashes with missing critical attribute
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        test_processed_crash.json_dump.cpu_arch = 'x86'
        test_processed_crash.success = True
        test_processed_crash.additional_minidumps = ['a', 'b', 'c']
        test_processed_crash.a = DotDict()
        test_processed_crash.a.success = True
        test_processed_crash.b = DotDict()
        test_processed_crash.b.success = True
        test_processed_crash.c = DotDict()
        test_processed_crash.c.success = False
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # find crashes with missing critical attribute
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        test_processed_crash.json_dump.cpu_arch = 'x86'
        test_processed_crash.success = True
        test_processed_crash.additional_minidumps = ['a', 'b', 'c']
        test_processed_crash.a = DotDict()
        test_processed_crash.a.success = True
        test_processed_crash.b = DotDict()
        test_processed_crash.b.success = True
        test_processed_crash.c = DotDict()
        test_processed_crash.c.success = False
        self.assertTrue(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

        # reject the perfect crash
        test_raw_crash = DotDict()
        test_raw_crash.process_type = 'plugin'
        test_raw_crash.ProductName = "Firefox"
        test_raw_crash.Version = '19'
        test_raw_crash.BuildID = '20121031'
        test_processed_crash = DotDict()
        test_processed_crash.dump = 'fake dump'
        test_processed_crash.json_dump = DotDict()
        test_processed_crash.json_dump.cpu_arch = 'x86'
        test_processed_crash.success = True
        test_processed_crash.additional_minidumps = ['a', 'b', 'c']
        test_processed_crash.a = DotDict()
        test_processed_crash.a.success = True
        test_processed_crash.b = DotDict()
        test_processed_crash.b.success = True
        test_processed_crash.c = DotDict()
        test_processed_crash.c.success = True
        self.assertFalse(filter_rule.predicate(
            test_raw_crash,
            test_processed_crash,
            fake_processor
        ))

class TestUpdateWindowAttributes(unittest.TestCase):

    def test_action_success(self):
        jd = copy.deepcopy(cannonical_json_dump)
        jd['threads'][0]['frames'][1]['function'] = \
            "F_1152915508___________________________________"
        jd['threads'][0]['frames'][3]['function'] = \
            "mozilla::plugins::PluginInstanceChild::UpdateWindowAttributes" \
                "(bool)"
        jd['threads'][0]['frames'][5]['function'] = \
            "mozilla::ipc::RPCChannel::Call(IPC::Message*, IPC::Message*)"
        pc = DotDict()
        pc.plugin = DotDict()
        pc.plugin.json_dump = jd

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = UpdateWindowAttributes()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertTrue('skunk_works' in pc['classifications'])

    def test_action_wrong_order(self):
        jd = copy.deepcopy(cannonical_json_dump)
        jd['threads'][0]['frames'][4]['function'] = \
            "F_1152915508___________________________________"
        jd['threads'][0]['frames'][3]['function'] = \
            "mozilla::plugins::PluginInstanceChild::UpdateWindowAttributes" \
                "(bool)"
        jd['threads'][0]['frames'][5]['function'] = \
            "mozilla::ipc::RPCChannel::Call(IPC::Message*, IPC::Message*)"
        pc = DotDict()
        pc.plugin = DotDict()
        pc.plugin.json_dump = jd

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = UpdateWindowAttributes()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertFalse(action_result)
        self.assertFalse('classifications' in pc)



class TestSetWindowPos(unittest.TestCase):

    def test_action_case_1(self):
        """sentinel exsits in stack, but no secondaries"""
        pc = DotDict()
        pc.plugin = DotDict()
        pijd = copy.deepcopy(cannonical_json_dump)
        pc.plugin.json_dump = pijd
        pc.plugin.json_dump['threads'][0]['frames'][2]['function'] = \
            'NtUserSetWindowPos'
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SetWindowPos()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertTrue('skunk_works' in pc.classifications)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'NtUserSetWindowPos | other'
        )

    def test_action_case_2(self):
        """sentinel exsits in stack, plus one secondary"""
        pc = DotDict()
        pc.plugin = DotDict()
        pijd = copy.deepcopy(cannonical_json_dump)
        pc.plugin.json_dump = pijd
        pc.plugin.json_dump['threads'][0]['frames'][2]['function'] = \
            'NtUserSetWindowPos'
        pc.plugin.json_dump['threads'][0]['frames'][4]['function'] = \
            'F_1378698112'
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SetWindowPos()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertTrue('skunk_works' in pc.classifications)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'NtUserSetWindowPos | F_1378698112'
        )

    def test_action_case_3(self):
        """nothing in 1st dump, sentinel and secondary in flash2 dump"""
        pc = DotDict()
        pc.plugin = DotDict()
        pijd = copy.deepcopy(cannonical_json_dump)
        pc.plugin.json_dump = pijd
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.json_dump['threads'][0]['frames'][2]['function'] = \
            'NtUserSetWindowPos'
        pc.flash2.json_dump['threads'][0]['frames'][4]['function'] = \
            'F455544145'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SetWindowPos()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertTrue('skunk_works' in pc.classifications)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'NtUserSetWindowPos | F455544145'
        )

    def test_action_case_4(self):
        """nothing in 1st dump, sentinel but no secondary in flash2 dump"""
        pc = DotDict()
        pc.plugin = DotDict()
        pijd = copy.deepcopy(cannonical_json_dump)
        pc.plugin.json_dump = pijd
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.json_dump['threads'][0]['frames'][2]['function'] = \
            'NtUserSetWindowPos'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SetWindowPos()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertTrue('skunk_works' in pc.classifications)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'NtUserSetWindowPos | other'
        )

    def test_action_case_5(self):
        """nothing in either dump"""
        pc = DotDict()
        pc.plugin = DotDict()
        pijd = copy.deepcopy(cannonical_json_dump)
        pc.plugin.json_dump = pijd
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SetWindowPos()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertFalse(action_result)
        self.assertFalse('classifications' in pc)



class TestSendWaitReceivePort(unittest.TestCase):

    def test_action_case_1(self):
        """success - target found in top 5 frames of stack"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.json_dump['threads'][0]['frames'][2]['function'] = \
            'NtAlpcSendWaitReceivePort'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SendWaitReceivePort()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)

    def test_action_case_2(self):
        """failure - target not found in top 5 frames of stack"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.json_dump['threads'][0]['frames'][6]['function'] = \
            'NtAlpcSendWaitReceivePort'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = SendWaitReceivePort()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertFalse(action_result)
        self.assertFalse('classifications' in pc)



class TestBug811804(unittest.TestCase):

    def test_action_success(self):
        """success - target signature fonud"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.signature = 'hang | NtUserWaitMessage | F34033164' \
                              '________________________________'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = Bug811804()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'bug811804-NtUserWaitMessage'
        )

    def test_action_failure(self):
        """success - target signature not found"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.signature = 'lars was here'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = Bug811804()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertFalse(action_result)
        self.assertFalse('classifications' in pc)


class TestBug812318(unittest.TestCase):

    def test_action_case_1(self):
        """success - both targets found in top 5 frames of stack"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.json_dump['threads'][0]['frames'][1]['function'] = \
            'NtUserPeekMessage'
        pc.flash2.json_dump['threads'][0]['frames'][2]['function'] = \
            'F849276792______________________________'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = Bug812318()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'bug812318-PeekMessage'
        )

    def test_action_case_2(self):
        """success - only 1st target found in top 5 frames of stack"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd
        pc.flash2.json_dump['threads'][0]['frames'][1]['function'] = \
            'NtUserPeekMessage'

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = Bug812318()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertTrue(action_result)
        self.assertTrue('classifications' in pc)
        self.assertEqual(
            pc.classifications.skunk_works.classification,
            'NtUserPeekMessage-other'
        )

    def test_action_case_3(self):
        """failure - no targets found in top 5 frames of stack"""
        pc = DotDict()
        f2jd = copy.deepcopy(cannonical_json_dump)
        pc.flash2 = DotDict()
        pc.flash2.json_dump = f2jd

        faked_processor = DotDict()
        faked_processor.c_signature_tool = c_signature_tool

        rc = DotDict()

        rule = Bug812318()
        action_result = rule.action(rc, pc, faked_processor)

        self.assertFalse(action_result)
        self.assertFalse('classifications' in pc)



