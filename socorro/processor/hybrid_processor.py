# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""this file defines the method of converting a raw crash into a processed
crash using the traditional algorithm used from 2008 through 2012."""

import re
import os
import subprocess
import datetime
import time
import json
from urllib import unquote_plus
from contextlib import closing, contextmanager

from configman import Namespace, RequiredConfig
from configman.converters import class_converter

from socorro.lib.datetimeutil import utc_now
from socorro.external.postgresql.dbapi2_util import (
    execute_no_results,
    execute_query_fetchall,
)
from socorro.processor.legacy_processor import LegacyCrashProcessor
from socorro.external.postgresql.connection_context import ConnectionContext
from socorro.lib.transform_rules import TransformRuleSystem
from socorro.lib.datetimeutil import datetimeFromISOdateString, UTC
from socorro.lib.ooid import dateFromOoid
from socorro.lib.util import (
    DotDict,
    emptyFilter,
    StrCachingIterator
)
from socorro.processor.breakpad_pipe_to_json import pipe_dump_to_json_dump

#==============================================================================
class IteratorWrapperWithPushBack(object):
    """simple wrapper for an iterator that allows push back of a
    value to the iterator.  The pushed back value will be the next value
    that the iterator yields"""
    #--------------------------------------------------------------------------
    def __init__(self, an_iterator):
        self.iterator = an_iterator  # the wrapped iterator
        self.has_pushback = False    # True if there is a pushback value
        self.pushback_value = None   # the value to return on next iter
            
    #--------------------------------------------------------------------------
    def __iter__(self):
        for x in self.iterator:
            # push backs can happen between yields so we must be prepared to
            # immediately yield a new pushback even after having just
            # yielded one.
            while self.has_pushback: 
                next_value = self.pushback_value
                self.pushback_value = None
                self.has_pushback = False
                yield next_value
            yield x

    #--------------------------------------------------------------------------
    def push_back(self, value):
        self.has_pushback = True
        self.pushback_value = value


#==============================================================================
class HybridCrashProcessor(LegacyCrashProcessor):
    """this class is a refactoring of the original processor algorithm into
    a single class.  This class is suitable for use in the 'processor_app'
    introducted in 2012."""

    required_config = Namespace()
    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(HybridCrashProcessor, self).__init__(
            config, 
            quit_check_callback
        )

    #--------------------------------------------------------------------------
    def _invoke_minidump_stackwalk(self, dump_pathname):
        """ This function invokes breakpad_stackdump as an external process
        capturing and returning the text output of stdout.  This version
        represses the stderr output.

              input parameters:
                dump_pathname: the complete pathname of the dumpfile to be
                               analyzed
        """
        command_line = self.mdsw_command_line.replace("DUMPFILEPATHNAME",
                                                      dump_pathname)
        subprocess_handle = subprocess.Popen(
            command_line,
            shell=True,
            stdout=subprocess.PIPE
        )
        self.config.logger.debug('STACKWALKER STARTS %s', command_line)
        return (
            IteratorWrapperWithPushBack(
                StrCachingIterator(subprocess_handle.stdout)
            ),
            subprocess_handle
        )

    #--------------------------------------------------------------------------
    def _stackwalk_analysis(
        self,
        dump_analysis_line_iterator,
        mdsw_subprocess_handle,
        crash_id,
        is_hang,
        java_stack_trace,
        submitted_timestamp,
        processor_notes
    ):
        with closing(dump_analysis_line_iterator) as mdsw_iter:
            processed_crash_update = self._analyze_header(
                crash_id,
                mdsw_iter,
                submitted_timestamp,
                processor_notes
            )
            crashed_thread = processed_crash_update.crashedThread
            try:
                make_modules_lowercase = \
                    processed_crash_update.os_name in ('Windows NT')
            except (KeyError, TypeError):
                make_modules_lowercase = True
            processed_crash_from_frames = self._analyze_frames(
                is_hang,
                java_stack_trace,
                make_modules_lowercase,
                mdsw_iter,
                submitted_timestamp,
                crashed_thread,
                processor_notes
            )
            processed_crash_update.update(processed_crash_from_frames)
            if "====PIPE DUMP ENDS===" in mdsw_iter.cache[-1]:
                # skip the sentinel between the sections if it is present
                # in the cache
                pipe_dump_str = ('\n'.join(mdsw_iter.cache[:-1]))
            else:
                pipe_dump_str = ('\n'.join(mdsw_iter.cache))
            processed_crash_update.dump = pipe_dump_str

            json_dump_lines = []
            for x in mdsw_iter:
                json_dump_lines.append(x)
            json_dump_str = ''.join(json_dump_lines)
            try:
                processed_crash_update.json_dump = json.loads(json_dump_str)
            except ValueError, x:
                processed_crash_update.json_dump = {}
                processor_notes.append(
                    "error reading MDSW json output: %s" % str(x)
                )
            try:
                processed_crash_update.exploitability = (
                    processed_crash_update.json_dump
                        ['sensitive']['exploitability']
                )
            except KeyError:
                processed_crash_update.exploitability = 'unknown'
                processor_notes.append("exploitablity information missing")
            mdsw_error_string = processed_crash_update.json_dump.setdefault(
                'status',
                None
            )

        return_code = mdsw_subprocess_handle.wait()
        if ((return_code is not None and return_code != 0) or
              mdsw_error_string != 'OK'):
            self._statistics.incr('mdsw_failures')
            if mdsw_error_string is None:
                mdsw_error_string = 'MDSW:%d' % return_code
            processor_notes.append(
                "MDSW failed: %s" % mdsw_error_string
            )
            processed_crash_update.success = False
            if processed_crash_update.signature.startswith("EMPTY"):
                processed_crash_update.signature = (
                    "%s; %s" % (
                        processed_crash_update.signature,
                        mdsw_error_string
                    )
                )
        return processed_crash_update

    #--------------------------------------------------------------------------
    def _analyze_header(self, crash_id, dump_analysis_line_iterator,
                        submitted_timestamp, processor_notes):
        """ Scan through the lines of the dump header:
            - extract data to update the record for this crash in 'reports',
              including the id of the crashing thread
            Returns: Dictionary of the various values that were updated in
                     the database
            Input parameters:
            - dump_analysis_line_iterator - an iterator object that feeds lines
                                            from crash dump data
            - submitted_timestamp
            - processor_notes
        """
        crashed_thread = None
        processed_crash_update = DotDict()
        # minimal update requirements
        processed_crash_update.success = True
        processed_crash_update.os_name = None
        processed_crash_update.os_version = None
        processed_crash_update.cpu_name = None
        processed_crash_update.cpu_info = None
        processed_crash_update.reason = None
        processed_crash_update.address = None

        header_lines_were_found = False
        flash_version = None
        for line in dump_analysis_line_iterator:
            if '====PIPE DUMP ENDS===' in line:
                dump_analysis_line_iterator.push_back(line)
                break
            line = line.strip()
            # empty line separates header data from thread data
            if line == '':
                break
            header_lines_were_found = True
            values = map(lambda x: x.strip(), line.split('|'))
            if len(values) < 3:
                processor_notes.append('Bad MDSW header line "%s"'
                                       % line)
                continue
            values = map(emptyFilter, values)
            if values[0] == 'OS':
                name = self._truncate_or_none(values[1], 100)
                version = self._truncate_or_none(values[2], 100)
                processed_crash_update.os_name = name
                processed_crash_update.os_version = version
            elif values[0] == 'CPU':
                processed_crash_update.cpu_name = \
                    self._truncate_or_none(values[1], 100)
                processed_crash_update.cpu_info = \
                    self._truncate_or_none(values[2], 100)
                try:
                    processed_crash_update.cpu_info = ('%s | %s' % (
                        processed_crash_update.cpu_info,
                        self._get_truncate_or_none(values, 3, 100)
                    ))
                except IndexError:
                    pass
            elif values[0] == 'Crash':
                processed_crash_update.reason = \
                    self._truncate_or_none(values[1], 255)
                try:
                    processed_crash_update.address = \
                        self._truncate_or_none(values[2], 20)
                except IndexError:
                    processed_crash_update.address = None
                try:
                    crashed_thread = int(values[3])
                except Exception:
                    crashed_thread = None
            elif values[0] == 'Module':
                # grab only the flash version, which is not quite as easy as
                # it looks
                if not flash_version:
                    flash_version = self._get_flash_version(values)
        if not header_lines_were_found:
            processor_notes.append('MDSW emitted no header lines')

        if crashed_thread is None:
            processor_notes.append('MDSW did not identify the crashing thread')
        processed_crash_update.crashedThread = crashed_thread
        if not flash_version:
            flash_version = '[blank]'
        processed_crash_update.flash_version = flash_version
        #self.config.logger.debug(
        #  " updated values  %s",
        #  processed_crash_update
        #)
        return processed_crash_update

    #--------------------------------------------------------------------------
    def _analyze_frames(self, hang_type, java_stack_trace,
                        make_modules_lower_case,
                        dump_analysis_line_iterator, submitted_timestamp,
                        crashed_thread,
                        processor_notes):
        """ After the header information, the dump file consists of just frame
        information.  This function cycles through the frame information
        looking for frames associated with the crashed thread (determined in
        analyzeHeader).  Each frame from that thread is written to the database
        until it has found a maximum of ten frames.

               returns:
                 a dictionary will various values to be used to update report
                 in the database, including:
                   truncated - boolean: True - due to excessive length the
                                               frames of the crashing thread
                                               may have been truncated.
                   signature - string: an overall signature calculated for this
                                       crash
                   processor_notes - string: any errors or warnings that
                                             happened during the processing

               input parameters:
                 hang_type -  0: if this is not a hang
                            -1: if "HangID" present in json,
                                   but "Hang" was not present
                            "Hang" value: if "Hang" present - probably 1
                 java_stack_trace - a source for java lang signature
                                    information
                 make_modules_lower_case - boolean, should modules be forced to
                                    lower case for signature generation?
                 dump_analysis_line_iterator - an iterator that cycles through
                                            lines from the crash dump
                 submitted_timestamp
                 crashed_thread - the number of the thread that crashed - we
                                 want frames only from the crashed thread
                 processor_notes
        """
        #logger.info("analyzeFrames")
        frame_counter = 0
        crashing_thread_found = False
        is_truncated = False
        frame_lines_were_found = False
        signature_generation_frames = []
        topmost_sourcefiles = []
        if hang_type == 1:
            thread_for_signature = 0
        else:
            thread_for_signature = crashed_thread
        max_topmost_sourcefiles = 1  # Bug 519703 calls for just one.
                                     # Lets build in some flex
        # this loop cycles through the pDump frames looking for the crashed
        # thread so that it can generate a signature.  Once it finds that
        # data, it spools out the rest of the pDump frames section ignoring the
        # contents.
        for line in dump_analysis_line_iterator:
            # the hybrid stackwalker outputs both pDump and jDump forms
            # this is the sentinel between them indicating the end of the pDump
            if '====PIPE DUMP ENDS===' in line:
                break  # there is more data coming move on to the next stage
            if crashing_thread_found:
                # there's no need to examine the thread frames as we've already
                # found the frames needed to generate a signature.  Just spool
                # through the remaining frame lines.
                continue
            frame_lines_were_found = True
            line = line.strip()
            if line == '':
                continue  # ignore unexpected blank lines
            (thread_num, frame_num, module_name, function, source, source_line,
             instruction) = [emptyFilter(x) for x in line.split("|")][:7]
            if len(topmost_sourcefiles) < max_topmost_sourcefiles and source:
                topmost_sourcefiles.append(source)
            if thread_for_signature == int(thread_num):
                if make_modules_lower_case:
                    try:
                        module_name = module_name.lower()
                    except AttributeError:
                        pass
                this_frame_signature = \
                    self.c_signature_tool.normalize_signature(
                        module_name,
                        function,
                        source,
                        source_line,
                        instruction
                    )
                signature_generation_frames.append(this_frame_signature)
                if (frame_counter ==
                        self.config.crashing_thread_frame_threshold):
                    processor_notes.append(
                        "MDSW emitted too many frames, triggering truncation"
                    )
                    dump_analysis_line_iterator.useSecondaryCache()
                    is_truncated = True
                frame_counter += 1
            elif frame_counter:
                # we've found the crashing thread, there is no need to
                # continue reading the pDump output
                # this boolean will force the loop to just consume the rest
                # of the pipe dump with no more processing.
                crashing_thread_found = True
        dump_analysis_line_iterator.stopUsingSecondaryCache()
        signature = self._generate_signature(signature_generation_frames,
                                             java_stack_trace,
                                             hang_type,
                                             crashed_thread,
                                             processor_notes)
        if not frame_lines_were_found:
            processor_notes.append("MDSW emitted no frames")
        return DotDict({
            "signature": signature,
            "truncated": is_truncated,
            "topmost_filenames": topmost_sourcefiles,
        })

