import datetime

from socorro.external.crashstorage_base import (
    CrashStorageBase
)
from configman import Namespace
from socorro.database.transaction_executor import (
    TransactionExecutor
)
from socorro.external.postgresql.connection_context import ConnectionContext
from socorro.lib.datetimeutil import uuid_to_date
from socorro.external.postgresql.dbapi2_util import (
    SQLDidNotReturnSingleValue,
    single_value_sql,
    execute_query_fetchall,
    execute_no_results,
)


#==============================================================================
class PostgreSQLCrashStorage(CrashStorageBase):
    """this implementation of crashstorage saves processed crashes to
    an instance of Postgresql.  It only saves certain key values to the
    partitioned reports table, therefore it is not a source for fetching
    complete processed reports and doesn't not implement any of the 'get'
    methods."""

    required_config = Namespace()

    required_config.add_option('transaction_executor_class',
                               #default=TransactionExecutorWithBackoff,
                               default=TransactionExecutor,
                               doc='a class that will manage transactions')
    required_config.add_option('database',
                               default=ConnectionContext,
                               doc='the class responsible for connecting to'
                               'Postgres')

    _reports_table_mappings = (
        # processed name, reports table name
        ("addons_checked", "addons_checked"),
        ("address", "address"),
        ("app_notes", "app_notes"),
        ("build", "build"),
        ("client_crash_date", "client_crash_date"),
        ("completeddatetime", "completed_datetime"),
        ("cpu_info", "cpu_info"),
        ("cpu_name", "cpu_name"),
        ("date_processed", "date_processed"),
        ("distributor", "distributor"),
        ("distributor_version", "distributor_version"),
        ("email", "email"),
        ("flash_version", "flash_version"),
        ("hangid", "hangid"),
        ("install_age", "install_age"),
        ("last_crash", "last_crash"),
        ("os_name", "os_name"),
        ("os_version", "os_version"),
        ("processor_notes", "processor_notes"),
        ("process_type", "process_type"),
        ("product", "product"),
        ("reason", "reason"),
        ("release_channel", "release_channel"),
        ("signature", "signature"),
        ("startedDateTime", "started_datetime"),
        ("success", "success"),
        ("topmost_filenames", "topmost_filenames"),
        ("truncated", "truncated"),
        ("uptime", "uptime"),
        ("user_comments", "user_comments"),
        ("user_id", "user_id"),
        ("url", "url"),
        ("uuid", "uuid"),
        ("version", "version"),
    )

    #--------------------------------------------------------------------------
    def __init__(self, config, quit_check_callback=None):
        super(PostgreSQLCrashStorage, self).__init__(
            config,
            quit_check_callback=quit_check_callback
        )
        self.database = config.database(config)
        self.transaction = config.transaction_executor_class(
            config,
            self.database,
            abandonment_callback=quit_check_callback
        )

    #--------------------------------------------------------------------------
    def save_processed(self, processed_crash):
        self.transaction(self._save_processed_transaction, processed_crash)

    #--------------------------------------------------------------------------
    def _save_processed_transaction(self, connection, processesd_crash):
        report_id = self._save_processed_report(connection, processesd_crash)
        self._save_plugins(connection, processesd_crash, report_id)
        self._save_extensions(connection, processesd_crash, report_id)

    #--------------------------------------------------------------------------
    def _save_processed_report(self, connection, processed_crash):
        column_list = []
        placeholder_list = []
        value_list = []
        for pro_crash_name, report_name in self._reports_table_mappings:
            column_list.append(report_name)
            placeholder_list.append('%s')
            value_list.append(processed_crash[pro_crash_name])
        ooid = processed_crash['uuid']
        reports_table_name = 'reports%s' % self._table_suffix_for_ooid(ooid)
        insert_sql = "insert into %s (%s) values (%s) returning id" % (
            reports_table_name,
            ', '.join(column_list),
            ', '.join(placeholder_list)
        )
        report_id = single_value_sql(connection, insert_sql, value_list)
        return report_id

    #--------------------------------------------------------------------------
    def _save_plugins(self, connection, processed_crash, report_id):
        """ Electrolysis Support - Optional - processed_crash may contain a
        ProcessType of plugin. In the future this value would be default,
        content, maybe even Jetpack... This indicates which process was the
        crashing process.
            plugin - When set to plugin, the jsonDocument MUST calso contain
                     PluginFilename, PluginName, and PluginVersion
        """
        process_type = processed_crash['process_type']
        if not process_type:
            return

        if process_type == "plugin":

            # Bug#543776 We actually will are relaxing the non-null policy...
            # a null filename, name, and version is OK. We'll use empty strings
            try:
                plugin_filename = processed_crash['PluginFilename']
                plugin_name = processed_crash['PluginName']
                plugin_version = processed_crash['PluginVersion']
            except KeyError, x:
                self.config.logger.error(
                    'the crash is missing a required field: %s', str(x)
                )
                return
            find_plugin_sql = ('select id from plugins '
                               'where filename = %s '
                               'and name = %s')
            try:
                plugin_id = single_value_sql(connection,
                                             find_plugin_sql,
                                             (plugin_filename,
                                              plugin_name))
            except SQLDidNotReturnSingleValue:
                insert_plugsins_sql = ("insert into plugins (filename, name) "
                                       "values (%s, %s) returning id")
                plugin_id = single_value_sql(connection,
                                             insert_plugsins_sql,
                                             (plugin_filename,
                                              plugin_name))
            ooid = processed_crash['uuid']
            table_suffix = self._table_suffix_for_ooid(ooid)
            plugin_reports_table_name = 'plugin_reports%s' % table_suffix
            plugins_reports_insert_sql = (
                'insert into %s '
                '    (report_id, plugin_id, date_processed, version) '
                'values '
                '    (%%s, %%s, %%s, %%s)' % plugin_reports_table_name
            )
            values_tuple = (report_id,
                            plugin_id,
                            processed_crash['date_processed'],
                            plugin_version)
            execute_no_results(connection,
                               plugins_reports_insert_sql,
                               values_tuple)

    #--------------------------------------------------------------------------
    def _save_extensions(self, connection, processed_crash, report_id):
        extensions = processed_crash['addons']
        ooid = processed_crash['uuid']
        table_suffix = self._table_suffix_for_ooid(ooid)
        extensions_table_name = 'extensions%s' % table_suffix
        extensions_insert_sql = (
            "insert into %s "
            "    (report_id, date_processed, extension_key, extension_id, "
            "     extension_version)"
            "values (%%s, %%s, %%s, %%s, %%s)" % extensions_table_name
        )
        for i, x in enumerate(extensions):
            try:
                execute_no_results(connection, extensions_insert_sql,
                                   (report_id,
                                    processed_crash['date_processed'],
                                    i,
                                    x[0][:100],
                                    x[1]))
            except IndexError:
                self.config.logger.warning(
                    '"%s" is deficient as a name and version for an addon',
                    str(x[0])
                )

    def new_ooids(self):
        for x in y:
            yield x

    #--------------------------------------------------------------------------
    def new_priority_jobs_iter (self):
        """Yields a list of ooids pulled from the 'jobs' table for all the jobs
        found in this process' priority jobs table.  If there are no priority
        jobs, it yields None. This iterator is perpetual - it never raises the
        StopIteration exception
        """
        get_priority_jobs_sql = (
          "select"
          "    j.id,"
          "    pj.uuid,"
          "    1,"
          "    j.starteddatetime "
          "from"
          "    jobs j right join %s pj on j.uuid = pj.uuid"
          % self.priorityJobsTableName
        )
        delete_one_priority_job_sql = (
          "delete from %s where uuid = %%s" % self.priorityJobsTableName
        )
        full_jobs_list = []
        while True:
            if not full_jobs_list:
                full_jobs_list = self.transaction(
                  execute_query_fetchall,
                  get_priority_jobs_sql
                )
            if full_jobs_list:
                while full_jobs_list:
                    an_ooid_tuple = full_jobs_list.pop(-1)
                    self.transaction(
                      execute_no_results,
                      delete_one_priority_job_sql,
                      (an_ooid_tuple[1],)
                    )
                    if an_ooid_tuple[0] is not None:
                        if an_ooid_tuple[3]:
                            continue  # already started via normal channels
                        else:
                            self.priority_job_set.add(an_ooid_tuple[1])
                            yield (an_ooid_tuple[0],an_ooid_tuple[1],an_ooid_tuple[2],)
                    else:
                        logger.debug("the priority job %s was never found", an_ooid_tuple[1])
            else:
                yield None

    #--------------------------------------------------------------------------
    def newNormalJobsIter (self):
        """
        Yields a list of job tuples pulled from the 'jobs' table for which the owner
        is this process and the started datetime is null.
        This iterator is perpetual - it never raises the StopIteration exception
        """
        getNormalJobSql = "select"  \
            "    j.id,"  \
            "    j.uuid,"  \
            "    priority "  \
            "from"  \
            "    jobs j "  \
            "where"  \
            "    j.owner = %d"  \
            "    and j.starteddatetime is null "  \
            "order by queueddatetime"  \
            "  limit %d" % (self.processorId,
                            self.config.batchJobLimit)
        normalJobsList = []
        while True:
            if not normalJobsList:
                normalJobsList = self.sdb.transaction_execute_with_retry( \
                    self.databaseConnectionPool,
                    getNormalJobSql
                )
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
        priorityJobIter = self.new_priority_jobs_iter()
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
                    logger.debug("incomingJobStream yielding %s job %s", aJobType, aJobTuple[1])
                    yield aJobTuple
                else:
                    logger.debug("Skipping already seen job %s", aJobTuple[1])
            else:
                logger.info("no jobs to do - sleeping %d seconds", self.processorLoopTime)
                seenUuids = set()
                self.responsiveSleep(self.processorLoopTime)


    #--------------------------------------------------------------------------
    @staticmethod
    def _table_suffix_for_ooid(ooid):
        """given an ooid, return the name of its storage table"""
        ooid_date = uuid_to_date(ooid)
        previous_monday_date = (ooid_date +
                                datetime.timedelta(days=-ooid_date.weekday()))
        return '%4d%02d%02d' % (previous_monday_date.year,
                                previous_monday_date.month,
                                previous_monday_date.day)