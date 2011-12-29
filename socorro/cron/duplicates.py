#!/usr/bin/python

import logging
from datetime import datetime
from datetime import timedelta
from socorro.lib.datetimeutil import utctz

logger = logging.getLogger("duplicates")

import socorro.lib.psycopghelper as psy
import socorro.lib.util as util

#-----------------------------------------------------------------------------------------------------------------
def find_duplicates(config):
  databaseConnectionPool = psy.DatabaseConnectionPool(config.databaseHost, config.databaseName, config.databaseUserName, config.databasePassword, logger)
  try:
    connection, cursor= databaseConnectionPool.connectionCursorPair()

    startTime = datetime.now(utctz) - timedelta(hours=3)
    endTime = startTime + timedelta(hours=1)
    cursor.callproc('update_reports_duplicates', (startTime, endTime))
    connection.commit()

    startTime += timedelta(minutes=30)
    endTime = startTime + timedelta(hours=1)
    cursor.callproc('update_reports_duplicates', (startTime, endTime))
    connection.commit()
  finally:
    databaseConnectionPool.cleanup()

