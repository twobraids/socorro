#!/usr/bin/python

import logging
from datetime import datetime
from datetime import timedelta

logger = logging.getLogger("hangReport")

import socorro.lib.psycopghelper as psy
import socorro.lib.util as util
from socorro.lib.datetimeutil import utctz


#-----------------------------------------------------------------------------------------------------------------
def run(config):
  databaseConnectionPool = psy.DatabaseConnectionPool(config.databaseHost, config.databaseName, config.databaseUserName, config.databasePassword, logger)
  try:
    connection, cursor = databaseConnectionPool.connectionCursorPair()

    startTime = datetime.now(utctz) - timedelta(days=1)
    cursor.callproc('update_hang_report', [startTime])
    connection.commit()
  finally:
    databaseConnectionPool.cleanup()

