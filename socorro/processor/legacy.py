from configman import Namespace, RequiredConfig
from configman.converters import class_converter

from socorro.lib.datetimeutil import utc_now
from socorro.external.postgresql.dbapi2_util import (
    execute_no_results,
    execute_query_fetchall,
)
from socorro.external.postgresql.connection_context import ConnectionContext
from socorro.database.transaction_executor import TransactionExecutor
from socorro.lib.transform_rules import TransformRuleSystem
from socorro.lib.datetimeutil import datetimeFromISOdateString
from socorro.lib.ooid import dateFromOoid
from socorre.lib.util import (
    lookupLimitedStringOrNone,
    DotDict,
    emptyFilter
)


#==============================================================================
class LegacyOoidSource(RequiredConfig):
    required_config = Namespace()
    required_config.add_option(
        'database_class',
        doc="the class of the database",
        default=ConnectionContext,
        from_string_converter=class_converter
    )
    required_config.add_option(
        'transaction_executor_class',
        default=TransactionExecutor,
        doc='a class that will manage transactions',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'batchJobLimit',
        default=10000,
        doc='the number of jobs to pull in a time',
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, processor_name):
        super(LegacyOoidSource)
        self.transaction = self.config.transaction_executor_class(config)
        self.database = self.config.database_class(config)
        self.processor_name = processor_name

    #--------------------------------------------------------------------------
    def newPriorityJobsIter (self):
        """
        Yields a list of JobTuples pulled from the 'jobs' table for all the
        jobs found in this process' priority jobs table.  If there are no
        priority jobs, yields None.  This iterator is perpetual - it never
        raises the StopIteration exception
        """
        getPriorityJobsSql =  (
            "select"
            "    j.id,"
            "    pj.uuid,"
            "    1,"
            "    j.starteddatetime "
            "from"
            "    jobs j right join %s pj on j.uuid = pj.uuid"
            % self.priorityJobsTableName)
        deleteOnePriorityJobSql = "delete from %s where uuid = %%s" %  \
            self.priorityJobsTableName
        fullJobsList = []
        while True:
            if not fullJobsList:
                fullJobsList = self.transaction(
                    execute_query_fetchall,
                    getPriorityJobsSql
                )
                #fullJobsList = self.sdb.transaction_execute_with_retry(
                    #self.databaseConnectionPool,
                    #getPriorityJobsSql)
            if fullJobsList:
                while fullJobsList:
                    aFullJobTuple = fullJobsList.pop(-1)
                    self.transaction(
                        execute_no_results,
                        deleteOnePriorityJobSql,
                        (aFullJobTuple[1],)
                    )
                    #self.sdb.transaction_execute_with_retry(
                            #self.databaseConnectionPool,
                            #deleteOnePriorityJobSql,
                            #(aFullJobTuple[1],))
                    if aFullJobTuple[0] is not None:
                        if aFullJobTuple[3]:
                            continue # the job already started
                        else:
                            self.priority_job_set.add(aFullJobTuple[1])
                            yield (aFullJobTuple[0],
                                   aFullJobTuple[1],
                                   aFullJobTuple[2],)
                    else:
                        self.config.logger.debug(
                            "the priority job %s was never found",
                            aFullJobTuple[1]
                        )
            else:
                yield None

    #--------------------------------------------------------------------------
    def newNormalJobsIter (self):
        """
        Yields a list of job tuples pulled from the 'jobs' table for which the
        owner is this process and the started datetime is null.  This iterator
        is perpetual - it never raises the StopIteration exception
        """
        getNormalJobSql = (
            "select"
            "    j.id,"
            "    j.uuid,"
            "    priority "
            "from"
            "    jobs j "
            "where"
            "    j.owner = %d"
            "    and j.starteddatetime is null "
            "order by queueddatetime"
            "  limit %d" % (self.processorId,
                            self.config.batchJobLimit))
        normalJobsList = []
        while True:
            if not normalJobsList:
                normalJobsList = self.transaction(
                    execute_query_fetchall,
                    getNormalJobSql
                )
                #normalJobsList = self.sdb.transaction_execute_with_retry( \
                    #self.databaseConnectionPool,
                    #getNormalJobSql
                #)
            if normalJobsList:
                while normalJobsList:
                    yield normalJobsList.pop(-1)
            else:
                yield None

    #--------------------------------------------------------------------------
    def incomingJobStream(self):
        """
           aJobTuple has this form: (jobId, jobUuid, jobPriority) ... of which
           jobPriority is pure excess, and should someday go away
           Yields the next job according to this pattern:
           START
           Attempt to yield a priority job
           If no priority job, attempt to yield a normal job
           If no priority or normal job, sleep self.processorLoopTime seconds
           loop back to START
        """
        priorityJobIter = self.newPriorityJobsIter()
        normalJobIter = self.newNormalJobsIter()
        seenUuids = set()
        while (True):
            aJobType = 'priority'
            self.quitCheck()
            self.checkin()
            aJobTuple = priorityJobIter.next()
            if not aJobTuple:
                aJobTuple = normalJobIter.next()
                aJobType = 'standard'
            if aJobTuple:
                if not aJobTuple[1] in seenUuids:
                    seenUuids.add(aJobTuple[1])
                    self.config.logger.debug(
                        "incomingJobStream yielding %s job %s",
                        aJobType,
                        aJobTuple[1]
                    )
                    yield aJobTuple
                else:
                    self.config.logger.debug(
                        "Skipping already seen job %s",
                        aJobTuple[1]
                    )
            else:
                # TODO: just yield a None - let the outer client do the delay
                self.config.logger.info(
                    "no jobs to do - sleeping %d seconds",
                    self.processorLoopTime
                )
                seenUuids = set()
                self.responsiveSleep(self.processorLoopTime)

    #--------------------------------------------------------------------------
    def __iter__(self):
        """an adapter that allows this class can serve as an iterator in a
        fetch_transform_save app"""
        for a_legacy_job_tuple in self.incomingJobStream():
            yield a_legacy_job_tuple[1]


#==============================================================================
class LegacyCrashProcessor(RequiredConfig):
    required_config = Namespace()
    required_config.add_option(
        'database_class',
        doc="the class of the database",
        default=ConnectionContext,
        from_string_converter=class_converter
    )
    required_config.add_option(
        'transaction_executor_class',
        default=TransactionExecutor,
        doc='a class that will manage transactions',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'stackwalk_command_line',
        doc='the template for the command to invoke minidump_stackwalk',
        default='$minidump_stackwalkPathname -m $dumpfilePathname '
        '$processorSymbolsPathnameList 2>/dev/null',
    )
    required_config.add_option(
        'minidump_stackwalkPathname',
        doc='the full pathname of the extern program minidump_stackwalk '
        '(quote path with embedded spaces)',
        default='/data/socorro/stackwalk/bin/minidump_stackwalk',
    )
    required_config.add_option(
        'symbolCachePath',
        doc='the path where the symbol cache is found (quote path with '
        'embedded spaces)',
        default='/mnt/socorro/symbols',
    )
    required_config.add_option(
        'processorSymbolsPathnameList',
        doc='comma or space separated list of symbol files for '
        'minidump_stackwalk (quote paths with embedded spaces)',
        default='/mnt/socorro/symbols/symbols_ffx,'
        '/mnt/socorro/symbols/symbols_sea,'
        '/mnt/socorro/symbols/symbols_tbrd,'
        '/mnt/socorro/symbols/symbols_sbrd,'
        '/mnt/socorro/symbols/symbols_os',
        from_string_converter=lambda x: x.replace(',', ' ')
    )
    required_config.add_option(
        'crashingThreadFrameThreshold',
        doc='the number of frames to keep in the raw dump for the '
        'crashing thread',
        default=100,
    )
    required_config.add_option(
        'crashingThreadTailFrameThreshold',
        doc='the number of frames to keep in the raw dump at the tail of the '
        'frame list',
        default=10,
    )
    required_config.add_option(
        'temporaryFileSystemStoragePath',
        doc='a local filesystem path where processor can write dumps '
        'temporarily for processing',
        default='/home/socorro/temp',
    )
    required_config.add_option(
        'c_signature_tool_class',
        doc='the class that can generate a C signature',
        default='socorro.processor.signature_utilities.CSignatureTool',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'java_signature_tool_class',
        doc='the class that can generate a Java signature',
        default='socorro.processor.signature_utilities.JavaSignatureTool',
        from_string_converter=class_converter
    )
    required_config.add_option(
        'knownFlashIdentifiers',
        doc='A subset of the known "debug identifiers" for flash versions, '
            'associated to the version',
        default={
            '7224164B5918E29AF52365AF3EAF7A500':'10.1.51.66',
            'C6CDEFCDB58EFE5C6ECEF0C463C979F80':'10.1.51.66',
            '4EDBBD7016E8871A461CCABB7F1B16120':'10.1',
            'D1AAAB5D417861E6A5B835B01D3039550':'10.0.45.2',
            'EBD27FDBA9D9B3880550B2446902EC4A0':'10.0.45.2',
            '266780DB53C4AAC830AFF69306C5C0300':'10.0.42.34',
            'C4D637F2C8494896FBD4B3EF0319EBAC0':'10.0.42.34',
            'B19EE2363941C9582E040B99BB5E237A0':'10.0.32.18',
            '025105C956638D665850591768FB743D0':'10.0.32.18',
            '986682965B43DFA62E0A0DFFD7B7417F0':'10.0.23',
            '937DDCC422411E58EF6AD13710B0EF190':'10.0.23',
            '860692A215F054B7B9474B410ABEB5300':'10.0.22.87',
            '77CB5AC61C456B965D0B41361B3F6CEA0':'10.0.22.87',
            '38AEB67F6A0B43C6A341D7936603E84A0':'10.0.12.36',
            '776944FD51654CA2B59AB26A33D8F9B30':'10.0.12.36',
            '974873A0A6AD482F8F17A7C55F0A33390':'9.0.262.0',
            'B482D3DFD57C23B5754966F42D4CBCB60':'9.0.262.0',
            '0B03252A5C303973E320CAA6127441F80':'9.0.260.0',
            'AE71D92D2812430FA05238C52F7E20310':'9.0.246.0',
            '6761F4FA49B5F55833D66CAC0BBF8CB80':'9.0.246.0',
            '27CC04C9588E482A948FB5A87E22687B0':'9.0.159.0',
            '1C8715E734B31A2EACE3B0CFC1CF21EB0':'9.0.159.0',
            'F43004FFC4944F26AF228334F2CDA80B0':'9.0.151.0',
            '890664D4EF567481ACFD2A21E9D2A2420':'9.0.151.0',
            '8355DCF076564B6784C517FD0ECCB2F20':'9.0.124.0',
            '51C00B72112812428EFA8F4A37F683A80':'9.0.124.0',
            '9FA57B6DC7FF4CFE9A518442325E91CB0':'9.0.115.0',
            '03D99C42D7475B46D77E64D4D5386D6D0':'9.0.115.0',
            '0CFAF1611A3C4AA382D26424D609F00B0':'9.0.47.0',
            '0F3262B5501A34B963E5DF3F0386C9910':'9.0.47.0',
            'C5B5651B46B7612E118339D19A6E66360':'9.0.45.0',
            'BF6B3B51ACB255B38FCD8AA5AEB9F1030':'9.0.28.0',
            '83CF4DC03621B778E931FC713889E8F10':'9.0.16.0',
        }
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(LegacyCrashProcessor, self).__init__(config)

        if quit_check_callback:
            self.quit_check = quit_check_callback
        else:
            self.quit_check = lambda: False
        self.transaction = self.config.transaction_executor_class(config)
        self.database = self.config.database_class(config)

        self.json_transform_rule_system = TransformRuleSystem()
        self.load_json_transform_rules()

        # *** from ExternalProcessor
        #preprocess the breakpad_stackwalk command line
        stripParensRE = re.compile(r'\$(\()(\w+)(\))')
        toPythonRE = re.compile(r'\$(\w+)')
        # Canonical form of $(param) is $param. Convert any that are needed
        tmp = stripParensRE.sub(r'$\2',config.stackwalkCommandLine)
        # Convert canonical $dumpfilePathname to DUMPFILEPATHNAME
        tmp = tmp.replace('$dumpfilePathname','DUMPFILEPATHNAME')
        # Convert canonical $processorSymbolsPathnameList to SYMBOL_PATHS
        tmp = tmp.replace('$processorSymbolsPathnameList','SYMBOL_PATHS')
        # finally, convert any remaining $param to pythonic %(param)s
        tmp = toPythonRE.sub(r'%(\1)s',tmp)
        self.commandLine = tmp % config
        # *** end from ExternalProcessor

    #--------------------------------------------------------------------------
    # instead, directly call the external function quit_check
    #def quitCheck(self):
        #self.quit_check()

    #--------------------------------------------------------------------------
    def __call__(self, raw_crash, raw_dump):
        self.processJob(raw_crash, raw_dump)

    #--------------------------------------------------------------------------
    def log_job_start(self, ooid):
        self.config.logger.info("starting job: %s", ooid)
        started_datetime = utc_now()
        self.transaction(
            execute_no_results,
            "update jobs set starteddatetime = %s where uuid = %s",
            (started_datetime, ooid)
        )

    #--------------------------------------------------------------------------
    def log_job_end(self, completed_datetime, success, ooid):
        self.config.logger.info(
            "finishing %s job: %s",
            'successful' if success else 'failed',
            ooid
        )
        started_datetime = utc_now()
        self.transaction(
            execute_no_results,
            "update jobs set completeddatetime = %s, success = %s "
                "where id = %s",
            (completed_datetime, success, ooid)
        )

    #--------------------------------------------------------------------------
    def processJob (self, raw_crash, raw_dump):
        """ This function is run only by a worker thread.
            Given a job, fetch a thread local database connection and the json
            document.  Use these to create the record in the 'reports' table,
            then start the analysis of the dump file.

            input parameters:
        """
        try:
            self.quit_check()
            ooid = raw_crash['ooid']
            processorErrorMessages = []

            self.log_job_start(ooid)

            self.config.logger.debug('about to apply rules')
            self.json_transform_rule_system.apply_all_rules(raw_crash, self)
            self.config.logger.debug('done applying transform rules')

            try:
                date_processed = datetimeFromISOdateString(
                  raw_crash["submitted_timestamp"]
                )
            except KeyError:
                date_processed = dateFromOoid(ooid)

            # formerly the call to 'insertReportIntoDatabase'
            processed_crash_dict = self.phase_one_transformation(
              ooid,
              raw_crash,
              date_processed,
              processorErrorMessages
            )


            try:
                temp_dump_pathname = self.dumpPathForUuid(ooid, raw_dump)
                #logger.debug('about to doBreakpadStackDumpAnalysis')
                isHang = ('hangid' in processed_crash_dict
                          and bool(processed_crash_dict['hangid']))
                # hangType values: -1 if old style hang with hangid
                #                        and Hang not present
                #                  else hangType == jsonDocument.Hang
                hangType = int(raw_crash.get("Hang", -1 if isHang else 0))
                java_stack_trace = raw_crash.setdefault('JavaStackTrace', None)
                processed_crash_update_dict = (
                    self._do_breakpad_stack_dump_analysis(
                        #reportId, # don't have it
                        ooid,
                        temp_dump_pathname,
                        hangType,
                        java_stack_trace,
                        #threadLocalCursor, # don't need it
                        date_processed,
                        processorErrorMessages)
                )
                processed_crash_dict.update(processed_crash_update_dict)
            finally:
                completedDateTime = utc_now()
                processed_crash_dict["completeddatetime"] = completedDateTime
                self.cleanup_temp_file(temp_dump_pathname)

            #logger.debug('finished a job - cleanup')
            #finished a job - cleanup
            # TODO: shouldn't this be at the end and not here?
            self.log_job_end(
                completedDateTime,
                processed_crash_dict['success'],
                ooid
            )

            # Bug 519703: Collect setting for topmost source filename(s), addon compatibility check override, flash version
            #reportsSql = """
        #update reports set
        #signature = %%s,
        #processor_notes = %%s,
        #started_datetime = timestamp with time zone %%s,
        #completed_datetime = timestamp with time zone %%s,
        #success = %%s,
        #truncated = %%s,
        #topmost_filenames = %%s,
        #addons_checked = %%s,
        #flash_version = %%s
        #where id = %s and date_processed = timestamp with time zone '%s'
        #""" % (reportId,date_processed)
            ##logger.debug("newReportRecordAsDict %s, %s", newReportRecordAsDict['topmost_filenames'], newReportRecordAsDict['flash_version'])
            ##topmost_filenames = "|".join(jsonDocument.get('topmost_filenames',[]))
            topmost_filenames = "|".join(
                processed_crash_dict.get('topmost_filenames',[])
            )
            addons_checked = None
            try:
                addons_checked_txt = raw_crash['EMCheckCompatibility'].lower()
                addons_checked = False
                if addons_checked_txt == 'true':
                    addons_checked = True
            except KeyError:
                pass # leaving it as None if not in the document

            try:
                processed_crash_dict['Winsock_LSP'] = raw_crash['Winsock_LSP']
            except KeyError:
                pass # if it's not in the original json,
                        # it does get into the jsonz

            #flash_version = processed_crash_dict.get('flash_version')

            #processor_notes = '; '.join(processorErrorMessages)
            #processed_crash_dict['processor_notes'] = processor_notes
            #infoTuple = (processed_crash_dict['signature'], processor_notes, startedDateTime, completedDateTime, processed_crash_dict["success"], processed_crash_dict["truncated"], topmost_filenames, addons_checked, flash_version)
            ##logger.debug("Updated report %s (%s): %s", reportId, jobUuid, str(infoTuple))
            #threadLocalCursor.execute(reportsSql, infoTuple)
            #threadLocalDatabaseConnection.commit()
            #self.saveProcessedDumpJson(processed_crash_dict, threadLocalCrashStorage)
            #self.submitOoidToElasticSearch(ooid)
            #if processed_crash_dict["success"]:
                #logger.info("succeeded and committed: %s", ooid)
            #else:
                #logger.info("failed but committed: %s", ooid)
            #self.quitCheck()
        except (KeyboardInterrupt, SystemExit):
            logger.info("quit request detected")
        except Exception, x:
            self.config.logger.warning(
                'Error while processing %s: %s',
                ooid,
                str(x),
                exc_info=True
            )
            processorErrorMessages.append(str(x))
            #message = '; '.join(processorErrorMessages).replace("'", "''")
            #processed_crash_dict['processor_notes'] = message
            #threadLocalCursor.execute("update jobs set completeddatetime = %s, success = False, message = %s where id = %s", (self.nowFunc(), message, jobId))
            #threadLocalDatabaseConnection.commit()
            #try:
                #threadLocalCursor.execute("update reports set started_datetime = timestamp with time zone %s, completed_datetime = timestamp with time zone %s, success = False, processor_notes = %s where id = %s and date_processed = timestamp with time zone %s", (startedDateTime, self.nowFunc(), message, reportId, date_processed))
                #threadLocalDatabaseConnection.commit()
                #self.saveProcessedDumpJson(processed_crash_dict, threadLocalCrashStorage)
            #except Exception, x:
                #sutil.reportExceptionAndContinue(logger)
                #threadLocalDatabaseConnection.rollback()

        processor_notes = '; '.join(processorErrorMessages)
        processed_crash_dict['processor_notes'] = processor_notes
        return processed_crash_dict


    #--------------------------------------------------------------------------
    def phase_one_transformation(self, uuid, raw_crash, date_processed,
                                 processor_notes):
        """
        This function is run only by a worker thread.
          Create the record for the current job in the 'reports' table
          input parameters:
            uuid: the unique id identifying the job - corresponds with the
                  uuid column in the 'jobs' and the 'reports' tables
            jsonDocument: an object with a dictionary interface for fetching
                          the components of the json document
            date_processed: when job came in (a key used in partitioning)
            processorErrorMessages: list of strings of error messages
        """
        #logger.debug("starting insertReportIntoDatabase")
        processed_crash = DotDict()
        processed_crash.uuid = uuid
        processed_crash.product = self.get_truncate_or_warn(
          raw_crash,
          'ProductName',
          processor_notes,
          None,
          30
        )
        processed_crash.version = self.get_truncate_or_warn(
          raw_crash,
          'Version',
          processor_notes,
          None,
          16
        )
        processed_crash.build = self.get_truncate_or_warn(
          raw_crash,
          'BuildID',
          processor_notes,
          None,
          16
        )
        processed_crash.url = self.get_truncate_or_none(
          raw_crash,
          'URL',
          255
        )
        processed_crash.user_comments = self.get_truncate_or_none(
          raw_crash,
          'Comments',
          500
        )
        processed_crash.app_notes = self.get_truncate_or_none(
          raw_crash,
          'Notes',
          1000
        )
        processed_crash.distributor = self.get_truncate_or_none(
          raw_crash,
          'Distributor',
          20
        )
        processed_crash.distributor_version = self.get_truncate_or_none(
          raw_crash,
          'Distributor_version',
          20
        )
        processed_crash.email = self.get_truncate_or_none(
          raw_crash,
          'Email',
          100
        )
        processed_crash.hangid = raw_crash.get('HangID',None)
        processed_crash.process_type = self.get_truncate_or_none(
          raw_crash,
          'ProcessType',
          10
        )
        processed_crash.release_channel = raw_crash.get(
          'ReleaseChannel',
          'unknown'
        )
        # userId is now deprecated and replace with empty string
        processed_crash.user_id = ""

        # ++++++++++++++++++++
        # date transformations
        processed_crash.date_processed = dateprocessed

        # defaultCrashTime: must have crashed before date processed
        date_processed_as_epoch = int(time.mktime(date_processed.timetuple()))
        timestampTime = int(
          raw_crash.get('timestamp', date_processed_as_epoch)
        ) # the old name for crash time
        crash_time = int(
          self.get_truncate_or_warn(
            raw_crash,
            'CrashTime',
            processor_notes,
            timestampTime,
            10
          )
        )
        processed_crash.crash_time = crash_time
        if crash_time == date_processed_as_epoch:
            processor_notes.append(
              "WARNING: No 'client_crash_date' "
              "could be determined from the raw_crash"
            )
        # StartupTime: must have started up some time before crash
        startupTime = int(raw_crash.get('StartupTime', crash_time))
        # InstallTime: must have installed some time before startup
        installTime = int(raw_crash.get('InstallTime', startupTime))
        processed_crash.client_crash_date = datetime.datetime.fromtimestamp(
          crash_time,
          UTC
        )
        processed_crash.install_age = crash_time - installTime
        processed_crash.uptime = max(0, crash_time - startupTime)
        try:
            last_crash = int(raw_crash['SecondsSinceLastCrash'])
        except:
            last_crash = None
        processed_crash.last_crash = last_crash

        #processed_crash_values =    (uuid,    crash_date,          date_processed,   product,   version,   buildID,  url,   install_age,   last_crash,   uptime,   email,   user_id,   user_comments,   app_notes,   distributor,   distributor_version,   None,                None,             None,            hangid,   process_type,   release_channel)
        #processed_crash_key_names = ("uuid", "client_crash_date", "date_processed", "product", "version", "build",  "url", "install_age", "last_crash", "uptime", "email", "user_id", "user_comments", "app_notes", "distributor", "distributor_version", "topmost_filenames", "addons_checked", "flash_version", "hangid", "process_type", "release_channel")
        #newReportRecordAsDict = dict(x for x in zip(processed_crash_key_names, processed_crash_values))

        # TODO: not sure how to reimplemnt this
        #if ooid in self.priority_job_set:
            #processorErrorMessages.append('Priority Job')
            #self.priority_job_set.remove(ooid)

        # can't get report id because we don't have the database here
        #reportId = processed_crash_dict["id"]
        processed_crash_dict['dump'] = ''
        processed_crash_dict["startedDateTime"] = startedDateTime

        try:
            processed_crash_dict["ReleaseChannel"] = \
                raw_crash["ReleaseChannel"]
        except KeyError:
            processed_crash_dict["ReleaseChannel"] = 'unknown'

        if self.config.collectAddon:
            #logger.debug("collecting Addons")
            # formerly 'insertAdddonsIntoDatabase'
            addonsAsAListOfTuples = self.process_extensions(
                raw_crash,
                date_processed,
                processorErrorMessages
            )
            processed_crash_dict["addons"] = addonsAsAListOfTuples

        if self.config.collectCrashProcess:
            #logger.debug("collecting Crash Process")
            # formerly insertCrashProcess
            crashProcessAsDict = self.do_process_type(
                raw_crash,
                date_processed,
                processorErrorMessages
            )
            processed_crash_dict.update(crashProcessAsDict)

        return processed_crash


    #--------------------------------------------------------------------------
    def process_extensions (self, jsonDocument, date_processed,
                            processorErrorMessages):
        jsonAddonString = self.get_truncate_or_warn(
          jsonDocument,
          'Add-ons',
          processorErrorMessages,
          ""
        )
        if not jsonAddonString: return []
        listOfAddonsForInput = [x.split(":")
                                for x in jsonAddonString.split(',')]
        listOfAddonsForOutput = []
        for i, x in enumerate(listOfAddonsForInput):
            try:
                listOfAddonsForOutput.append(x)
            except IndexError:
                processorErrorMessages.append(
                  '"%s" is deficient as a name and version for an addon' %
                  str(x[0])
                )
        return listOfAddonsForOutput

    #--------------------------------------------------------------------------
    def do_process_type (self, raw_crash,
                         date_processed, processorErrorMessages):
        """ Electrolysis Support - Optional - raw_crash may contain a
        ProcessType of plugin. In the future this value would be default,
        content, maybe even Jetpack... This indicates which process was the
        crashing process.
        """
        process_type_additions_dict = sutil.DotDict()
        process_type = lookupLimitedStringOrNone(raw_crash,
                                                 'ProcessType',
                                                 10)
        if not process_type:
            return process_type_additions_dict
        process_type_additions_dict.process_type = process_type

        #logger.debug('processType %s', processType)
        if process_type == 'plugin':
            # Bug#543776 We actually will are relaxing the non-null policy...
            # a null filename, name, and version is OK. We'll use empty strings
            process_type_additions_dict.PluginFilename = (
                lookupStringOrEmptyString(raw_crash, 'PluginFilename')
            )
            process_type_additions_dict.PluginName = (
                lookupStringOrEmptyString(raw_crash, 'PluginName')
            )
            process_type_additions_dict.PluginVersion = (
                lookupStringOrEmptyString(raw_crash, 'PluginVersion')
            )

        return process_type_additions_dict

    #--------------------------------------------------------------------------
    def _do_breakpad_stack_dump_analysis (self, uuid, dumpfilePathname,
                                          isHang, java_stack_trace,
                                          date_processed,
                                          processorErrorMessages):
        """ This function coordinates the steps of running the
        breakpad_stackdump process and analyzing the textual output for
        insertion into the database.

        returns:
          truncated - boolean: True - due to excessive length the frames of
                                      the crashing thread have been truncated.

        input parameters:
          uuid - the unique string identifier for the crash report
          dumpfilePathname - the complete pathname for the =crash dump file
          isHang - boolean, is this a hang crash?
          app_notes - a source for java signatures info
          databaseCursor - the cursor to use for insertion into the database
          date_processed
          processorErrorMessages
        """
        dumpAnalysisLineIterator, subprocessHandle = \
            self.invokeBreakpadStackdump(dumpfilePathname)
        dumpAnalysisLineIterator.secondaryCacheMaximumSize = \
            self.config.crashingThreadTailFrameThreshold + 1
        try:
            processed_crash_fragment_dict = self.analyzeHeader(
              dumpAnalysisLineIterator,
              date_processed,
              processorErrorMessages
            )
            crashedThread = processed_crash_fragment_dict["crashedThread"]
            try:
                lowercaseModules = \
                    processed_crash_fragment_dict['os_name'] in ('Windows NT')
            except KeyError:
                lowercaseModules = True
            evenMoreReportValuesAsDict = self.analyzeFrames(
              isHang,
              java_stack_trace,
              lowercaseModules,
              dumpAnalysisLineIterator,
              date_processed,
              crashedThread,
              processorErrorMessages
            )
            processed_crash_fragment_dict.update(evenMoreReportValuesAsDict)
            for x in dumpAnalysisLineIterator:
                pass  # need to spool out the rest of the stream so the
                      # cache doesn't get truncated
            dumpAnalysisAsString = ('\n'.join(dumpAnalysisLineIterator.cache))
            processed_crash_fragment_dict["dump"] = dumpAnalysisAsString
        finally:
            # this is really a handle to a file-like object - got to close it
            dumpAnalysisLineIterator.theIterator.close()
        returncode = subprocessHandle.wait()
        if returncode is not None and returncode != 0:
            processorErrorMessages.append(
              "%s failed with return code %s when processing dump %s" %
              (self.config.minidump_stackwalkPathname,
               subprocessHandle.returncode, uuid)
            )
            processed_crash_fragment_dict['success'] = False
            if processed_crash_fragment_dict["signature"].startswith("EMPTY"):
                processed_crash_fragment_dict["signature"] += "; corrupt dump"
        return processed_crash_fragment_dict

    #--------------------------------------------------------------------------
    def invokeBreakpadStackdump(self, dumpfilePathname):
        """ This function invokes breakpad_stackdump as an external process
        capturing and returning the text output of stdout.  This version
        represses the stderr output.

              input parameters:
                dumpfilePathname: the complete pathname of the dumpfile to be
                                  analyzed
        """
        #logger.debug("analyzing %s", dumpfilePathname)
        if isinstance(self.config.processorSymbolsPathnameList, list):
            symbol_path = ' '.join(
              ['"%s"' % x for x in self.config.processorSymbolsPathnameList]
            )
        else:
            symbol_path = ' '.join(
              ['"%s"' % x
               for x in self.config.processorSymbolsPathnameList.split()]
            )
        newCommandLine = self.commandLine.replace("DUMPFILEPATHNAME",
                                                  dumpfilePathname)
        newCommandLine = newCommandLine.replace("SYMBOL_PATHS", symbol_path)
        #logger.info("invoking: %s", newCommandLine)
        subprocessHandle = subprocess.Popen(
          newCommandLine,
          shell=True,
          stdout=subprocess.PIPE
        )
        return (socorro.lib.util.StrCachingIterator(subprocessHandle.stdout),
                subprocessHandle)

    #--------------------------------------------------------------------------
    def analyzeHeader(self, dumpAnalysisLineIterator, date_processed,
                      processorErrorMessages):
        """ Scan through the lines of the dump header:
            - # deprecated: extract the information for populating the
                            'modules' table
            - extract data to update the record for this crash in 'reports',
              including the id of the crashing thread
            Returns: Dictionary of the various values that were updated in
                     the database
            Input parameters:
            - dumpAnalysisLineIterator - an iterator object that feeds lines
                                         from crash dump data
            - date_processed
            - processorErrorMessages
        """
        #logger.info("analyzeHeader")
        crashedThread = None
        moduleCounter = 0
        reportUpdateValues = {"id": reportId, "success": True}

        analyzeReturnedLines = False
        reportUpdateSqlParts = []
        flash_version = None
        for lineNumber, line in enumerate(dumpAnalysisLineIterator):
            line = line.strip()
            # empty line separates header data from thread data
            if line == '':
                break
            analyzeReturnedLines = True
            #logger.debug("[%s]", line)
            values = map(lambda x: x.strip(), line.split('|'))
            if len(values) < 3:
                processorErrorMessages.append('Cannot parse header line "%s"'
                                              % line)
                continue
            values = map(socorro.lib.util.emptyFilter, values)
            if values[0] == 'OS':
                name = self.get_truncate_or_none(values[1], 100)
                version = self.get_truncate_or_none(values[2], 100)
                reportUpdateValues['os_name']=name
                reportUpdateValues['os_version']=version
                reportUpdateSqlParts.extend(
                  ['os_name = %(os_name)s', 'os_version = %(os_version)s']
                )
                #osId = self.idCache.getOsId(name,version)
                #reportUpdateValues['osdims_id'] = osId
                #reportUpdateSqlParts.append('osdims_id = %(osdims_id)s')
            elif values[0] == 'CPU':
                reportUpdateValues['cpu_name'] = \
                    self.get_truncate_or_none(values[1], 100)
                reportUpdateValues['cpu_info'] = \
                    self.get_truncate_or_none(values[2], 100)
                try:
                    reportUpdateValues['cpu_info'] = (
                      '%s | %s' % (reportUpdateValues['cpu_info'],
                                   self.get_truncate_or_none(values[3], 100)))
                except IndexError:
                    pass
                reportUpdateSqlParts.extend(['cpu_name = %(cpu_name)s',
                                             'cpu_info = %(cpu_info)s'])
            elif values[0] == 'Crash':
                reportUpdateValues['reason'] = \
                    self.get_truncate_or_none(values[1], 255)
                reportUpdateValues['address'] = \
                    self.get_truncate_or_none(values[2], 20)
                reportUpdateSqlParts.extend(['reason = %(reason)s',
                                             'address = %(address)s'])
                try:
                    crashedThread = int(values[3])
                except Exception:
                    crashedThread = None
            elif values[0] == 'Module':
                # grab only the flash version, which is not quite as easy as
                # it looks
                if not flash_version:
                    flash_version = self.getVersionIfFlashModule(values)
        if not analyzeReturnedLines:
            message = "%s returned no header lines for reportid: %s" % \
                (self.config.minidump_stackwalkPathname, reportId)
            processorErrorMessages.append(message)
            logger.warning("%s", message)

        #logger.info('reportUpdateValues: %s', str(reportUpdateValues))
        #logger.info('reportUpdateSqlParts: %s', str(reportUpdateSqlParts))
        #if len(reportUpdateSqlParts) > 1:
            #reportUpdateSQL = """update reports set %s where id=%%(id)s AND date_processed = timestamp with time zone '%s'"""%(",".join(reportUpdateSqlParts),date_processed)
            #databaseCursor.execute(reportUpdateSQL, reportUpdateValues)

        if crashedThread is None:
            message = "No thread was identified as the cause of the crash"
            processorErrorMessages.append(message)
            logger.warning("%s", message)
        reportUpdateValues["crashedThread"] = crashedThread
        if not flash_version:
            flash_version = '[blank]'
        reportUpdateValues['flash_version'] = flash_version
        #logger.debug(" updated values  %s", reportUpdateValues)
        return reportUpdateValues

    #--------------------------------------------------------------------------
    flashRE = re.compile(r'NPSWF32_?(.*)\.dll|libflashplayer(.*)\.(.*)|'
                         'Flash ?Player-?(.*)')
    def getVersionIfFlashModule(self,moduleData):
        """If (we recognize this module as Flash and figure out a version):
        Returns version; else (None or '')"""
        try:
            module,filename,version,debugFilename,debugId = moduleData[:5]
        except ValueError:
            logger.debug("bad module line %s", moduleData)
            return None
        m = ProcessorWithExternalBreakpad.flashRE.match(filename)
        if m:
            if not version:
                groups = m.groups()
                if groups[0]:
                    version = groups[0].replace('_', '.')
                elif groups[1]:
                    version = groups[1]
                elif groups[3]:
                    version = groups[3]
                elif 'knownFlashDebugIdentifiers' in self.config:
                    version = \
                        self.config.knownFlashDebugIdentifiers.get(debugId)
        else:
            version = None
        return version

    #--------------------------------------------------------------------------
    def analyzeFrames(self, hangType, java_stack_trace, lowercaseModules,
                      dumpAnalysisLineIterator, date_processed, crashedThread,
                      processorErrorMessages):
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
                 hangType -  0: if this is not a hang
                            -1: if "HangID" present in json,
                                   but "Hang" was not present
                            "Hang" value: if "Hang" present - probably 1
                 java_stack_trace - a source for java lang signature
                                    information
                 lowerCaseModules - boolean, should modules be forced to lower
                                    case for signature generation?
                 dumpAnalysisLineIterator - an iterator that cycles through
                                            lines from the crash dump
                 date_processed
                 crashedThread - the number of the thread that crashed - we
                                 want frames only from the crashed thread
        """
        #logger.info("analyzeFrames")
        frameCounter = 0
        truncated = False
        analyzeReturnedLines = False
        signatureList = []
        topmost_sourcefiles = []
        if hangType == 1:
            thread_for_signature = 0
        else:
            thread_for_signature = crashedThread
        max_topmost_sourcefiles = 1 # Bug 519703 calls for just one.
                                    # Lets build in some flex
        for line in dumpAnalysisLineIterator:
            analyzeReturnedLines = True
            #logger.debug("  %s", line)
            line = line.strip()
            if line == '':
                processorErrorMessages.append("An unexpected blank line in "
                                              "this dump was ignored")
                continue  #some dumps have unexpected blank lines - ignore them
            (thread_num, frame_num, module_name, function, source, source_line,
             instruction) = [emptyFilter(x) for x in line.split("|")]
            if len(topmost_sourcefiles) < max_topmost_sourcefiles and source:
                topmost_sourcefiles.append(source)
            if thread_for_signature == int(thread_num):
                if frameCounter < 30:
                    if lowercaseModules:
                        try:
                            module_name = module_name.lower()
                        except AttributeError:
                            pass
                    thisFramesSignature = \
                        self.c_signature_tool.normalize_signature(
                          module_name,
                          function,
                          source,
                          source_line,
                          instruction
                        )
                    signatureList.append(thisFramesSignature)
                if frameCounter == self.config.crashingThreadFrameThreshold:
                    processorErrorMessages.append(
                      "This dump is too long and has triggered the automatic "
                      "truncation routine"
                    )
                    #self.configlogger.debug("starting secondary cache with "
                                             #"framecount = %d", frameCounter)
                    dumpAnalysisLineIterator.useSecondaryCache()
                    truncated = True
                frameCounter += 1
            elif frameCounter:
                break
        dumpAnalysisLineIterator.stopUsingSecondaryCache()
        signature = self.generate_signature(signatureList,
                                            java_stack_trace,
                                            hangType,
                                            crashedThread,
                                            processorErrorMessages)
        #self.configlogger.debug("  %s", (signature,
        #'; '.join(processorErrorMessages), reportId, date_processed))
        if not analyzeReturnedLines:
            message = "No frame data available"
            processorErrorMessages.append(message)
            logger.warning("%s", message)
        #processor_notes = '; '.join(processorErrorMessages)
        #databaseCursor.execute("update reports set signature = %%s, "
        #"processor_notes = %%s where id = %%s and date_processed = timestamp "
        #"with time zone '%s'" % (date_processed),(signature, processor_notes,"
        #"reportId))
        #logger.debug ("topmost_sourcefiles  %s", topmost_sourcefiles)
        return { "signature": signature,
                 "truncated": truncated,
                 "topmost_filenames":topmost_sourcefiles,
                 }

    #---------------------------------------------------------------------------
    def generate_signature(self,
                           signature_list,
                           java_stack_trace,
                           hang_type,
                           crashed_thread,
                           processor_notes_list,
                           signature_max_len=255):
        if java_stack_trace:
            # generate a Java signature
            signature, signature_notes = self.java_signature_tool.generate(
              java_stack_trace,
              delimiter=' '
            )
            return signature
        else:
            # generate a C signature
            signature, signature_notes = self.c_signature_tool.generate(
              signature_list,
              hang_type,
              crashed_thread
            )
        if signature_notes:
            processor_notes_list.extend(signature_notes)

        return signature

    #--------------------------------------------------------------------------
    def load_json_transform_rules(self):
        sql = ("select predicate, predicate_args, predicate_kwargs, "
               "       action, action_args, action_kwargs "
               "from transform_rules "
               "where "
               "  category = 'processor.json_rewrite'")
        try:
            rules = self.transaction(
                execute_query_fetchall,
                sql
            )
        except Exception:
            self.config.logger.info('Unable to load trasform rules from the'
                                    'database', exc_info=True)
            rules = [('socorro.processor.processor.json_equal_predicate',
                      '',
                      'key="ReleaseChannel", value="esr"',
                      'socorro.processor.processor.json_reformat_action',
                      '',
                      'key="Version", format_str="%(Version)sesr"'),
                     ('socorro.processor.processor.json_ProductID_predicate',
                      '',
                      '',
                      'socorro.processor.processor.'
                          'json_Product_rewrite_action',
                      '',
                      '') ]

        self.json_transform_rule_system.load_rules(rules)
        self.config.logger.info('done loading rules: %s',
                                str(self.json_transform_rule_system.rules))

    #--------------------------------------------------------------------------
    def dumpPathForUuid(self, ooid, raw_dump):
        base_path = self.config.
        dump_path = ("%s/%s.dump" % (base_path, ooid)).replace('//', '/')
        with open(dump_path, "w") as f:
            f.write(raw_dump)
        return dump_path

    #--------------------------------------------------------------------------
    def cleanup_temp_file(self, pathname):
        try:
            os.unlink(pathname)
        except IOError:
            self.config.logger.warning(
                'unable to delete %s. manual deletion is required.',
                pathname,
                exc_info=True
            )

    #--------------------------------------------------------------------------
    def get_truncate_or_warn(self, jsonDoc, key, errorMessageList,
                      default=None, maxLength=10000):
        try:
            return jsonDoc[key][:maxLength];
        except KeyError:
            errorMessageList.append("WARNING: raw_crash missing %s" % key)
            return default
        except TypeError, x:
            errorMessageList.append(
              "WARNING: raw_crash [%s] contains unexpected value: %s" %
                (key, x)
            )
            return default

    #--------------------------------------------------------------------------
    def get_truncate_or_none(self, a_mapping, key, maxLength=10000):
        try:
            return a_mapping[key][:maxLength];
        except (KeyError, IndexError, TypeError):
            return None


##=================================================================================================================
#class ProcessorWithExternalBreakpad (processor.Processor):
    #"""
    #"""
##-----------------------------------------------------------------------------------------------------------------
    #def __init__(self, config):
                #super(ProcessorWithExternalBreakpad, self).__init__(config)

                #assert "processorSymbolsPathnameList" in config, "processorSymbolsPathnameList is missing from the configuration"
                #assert "crashingThreadFrameThreshold" in config, "crashingThreadFrameThreshold is missing from the configuration"
                #assert "crashingThreadTailFrameThreshold" in config, "crashingThreadTailFrameThreshold is missing from the configuration"
                #assert "stackwalkCommandLine" in config, "stackwalkCommandLine is missing from the configuration"

                ##preprocess the breakpad_stackwalk command line
                #stripParensRE = re.compile(r'\$(\()(\w+)(\))')
                #toPythonRE = re.compile(r'\$(\w+)')
                ## Canonical form of $(param) is $param. Convert any that are needed
                #tmp = stripParensRE.sub(r'$\2',config.stackwalkCommandLine)
                ## Convert canonical $dumpfilePathname to DUMPFILEPATHNAME
                #tmp = tmp.replace('$dumpfilePathname','DUMPFILEPATHNAME')
                ## Convert canonical $processorSymbolsPathnameList to SYMBOL_PATHS
                #tmp = tmp.replace('$processorSymbolsPathnameList','SYMBOL_PATHS')
                ## finally, convert any remaining $param to pythonic %(param)s
                #tmp = toPythonRE.sub(r'%(\1)s',tmp)
                #self.commandLine = tmp % config