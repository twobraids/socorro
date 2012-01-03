"""
Refactor the stuff that sets up and tears down the test database details
"""
import datetime as dt
import logging
import os
import time
import psycopg2
import socorro.unittest.testlib.createJsonDumpStore as createJDS
from socorro.lib.datetimeutil import utctz

def datetimeNow(cursor):
  cursor.execute("SELECT LOCALTIMESTAMP(6)")
  return cursor.fetchone()[0]

def fillProcessorTable(cursor, processorCount, stamp=None, processorMap = {},logger = None):
  """
  Puts some entries into the processor table.
  Also creates priority_jobs_NNN for each processor id, unless that table exists
  Given a map of id->timestamp, sets the lastseendatetime for each successive processor to that stamp
  (Ignores ids generated by the count or in the processorMap, and uses database's serial id generator)
  """
  if not logger:
    logger = logging.getLogger()

  if not stamp: stamp = dt.datetime.now(utctz)
  if not processorCount and not processorMap: return
  sql = "INSERT INTO processors (name,startdatetime,lastseendatetime) VALUES (%s,%s,%s);"
  data = []
  if processorMap:
    data.extend([('test_%d'%(id),stamp,processorMap.get(id,stamp)) for id in processorMap.keys() ])
  else:
    data.extend([('test_%d'%(x),stamp, stamp) for x in range(1,processorCount+1) ])
  try:
    cursor.executemany(sql,data)
    cursor.connection.commit()

    sql = "SELECT id from processors;"
    cursor.execute(sql)
    allIds = cursor.fetchall()
    cursor.connection.rollback()
    sql = "CREATE TABLE priority_jobs_%s (uuid varchar(50) not null primary key);"
    for tup in allIds:
      try:
        cursor.execute(sql%(tup[0]))
        cursor.connection.commit()
      except psycopg2.ProgrammingError:
        cursor.connection.rollback()
  finally:
    cursor.connection.rollback()

def moreUuid():
  data = [ x for x in createJDS.jsonFileData.keys() ] # fixed order
  jdsIndex = 0
  hex4 = 'dead'
  currentHex = 0
  while True:
    if 0xffff == currentHex: currentHex = 0
    if jdsIndex >= len(data):
      jdsIndex = 0
      currentHex += 1
      if 0xdead == currentHex : currentHex += 1
      hex4 = "%04x"%currentHex
    yield data[jdsIndex].replace('dead',hex4)
    jdsIndex += 1

def makeJobDetails(idsMapToCounts):
  """
  Generate some bogus uuid and path data
  """
  data = []
  gen = moreUuid()
  for id in idsMapToCounts.keys():
    for x in range(idsMapToCounts[id]):
      uuid = gen.next()
      data.append(("/hmm/%s/%s/%s"%(uuid[:2],uuid[2:4],uuid),uuid,id,))
  return data

def addSomeJobs(cursor,idsMapToCounts, logger = None):
  """
  Insert the requested rows into jobs table.
  idsMapToCounts: id:countOfjobsForProcessorWithThisId
    BEWARE: The ids must be actual ids from the processors table or this will fail.
  returns list of the inserted job details
  """
  if not logger:
    logger = logging.getLogger()
  logger.debug("ADDING: %s"%(str(idsMapToCounts)))
  data = makeJobDetails(idsMapToCounts)
  sql = "INSERT INTO jobs (pathname,uuid,owner) VALUES (%s,%s,%s)"
  try:
    cursor.executemany(sql,data)
    cursor.connection.commit()
  except Exception,x:
    logger.error("Failed to addSomeJobs(%s): %s",str(idsMapToCounts),x)
    cursor.connection.rollback()
    raise x
  return data

def setPriority(cursor,jobIds,priorityTableName=None):
  """
  if priorityTableName: for each id in jobIds, insert row in that table holding the uuid of that job
  otherwise, set the job.priority column in the jobs table
    BEWARE: The job ids must be actual ids from the jobs table or this will quietly fail to do anything usefuld
  """
  if not jobIds: return
  wherePart = 'WHERE id IN (%s)'%(', '.join((str(x) for x in jobIds)))
  if priorityTableName:
    sql = "INSERT INTO %s (uuid) SELECT uuid FROM jobs %s"%(priorityTableName,wherePart)
  else:
    sql = "UPDATE jobs SET priority = 1 %s"%(wherePart)
  cursor.execute(sql)
  cursor.connection.commit()

dimsData = {
  'osdims': [
  {'os_name':'Windows NT','os_version':'5.1.2600 Service Pack 2',},
  {'os_name':'Windows NT','os_version':'6.0.6001 Service Pack 1',},
  {'os_name':'Windows NT','os_version':'6.1.7000',},
  {'os_name':'Mac OS X','os_version':'10.4.10 8R2218',},
  {'os_name':'Mac OS X','os_version':'10.5.6 9G2110',},
  {'os_name':'Linux','os_version':'2.6.28 i686',},
  {'os_name':'Linux','os_version':'2.6.27.21 i686',},
  ],
  'productdims': [
  {'product':'Firefox','version':'3.0.6','release':'major','branch':'1.9'},
  {'product':'Firefox','version':'3.0.8','release':'major','branch':'1,9'},
  {'product':'Firefox','version':'3.0.9','release':'major','branch':'1.9'},
  {'product':'Firefox','version':'3.1.1','release':'major','branch':'1.9.1'},
  {'product':'Firefox','version':'3.1.2b','release':'milestone','branch':'1.9.1'},
  {'product':'Firefox','version':'3.1.3b','release':'milestone','branch':'1.9.1'},
  {'product':'Firefox','version':'3.5b4pre','release':'development','branch':'1.9.1'},
  {'product':'Thunderbird','version':'2.0.0.21','release':'major','branch':'1.9.2'},
  ],

  'urldims': [
  {'domain':'www.mozilla.com','url':'http://www.mozilla.com/en-US/about/get-involved.html'},
  {'domain':'www.google.com','url':'http://www.google.com/search'},
  {'domain':'en.wikipedia.org','url':'http://en.wikipedia.org/wiki/Maroon'},
  {'domain':'www.exactitudes.nl','url':'http://www.exactitudes.nl/'},
  {'domain':'mac.appstorm.net','url':'http://mac.appstorm.net/category/reviews/internet-reviews/'},
  {'domain':'www.phys.ufl.edu','url':'http://www.phys.ufl.edu/~det/phy2060/heavyboots.html'},
  {'domain':'www.gracesmith.co.uk','url':'http://www.gracesmith.co.uk/84-amazingly-useful-css-tips-resources/'},
  {'domain':'www.picocat.com','url':'http://www.picocat.com/2009/03/how-to-wash-cat.html'},
  {'domain':'www.geeky-gadgets.com','url':'http://www.geeky-gadgets.com/necktie-spy-camera/'},
  {'domain':'operaphantom.blogsome.com','url':'http://operaphantom.blogsome.com/images/11111kant.jpg'},
  {'domain':'www.amazingplanetc.om','url':'http://www.amazing-planet.com/en/home'},
  {'domain':'www.myfavoriteword.com','url':'http://www.myfavoriteword.com/'},
  {'domain':'www.wildmoodswings.co.uk','url':'http://www.wildmoodswings.co.uk/'},
  ]
}

def fillDimsTables(cursor, data = None):
  """
  By default, use dimsData above. Otherwise, process the provided table.
  data must be of the form: {tablename:[{acolumn:avalue,bcolumn:bvalue,...},...],anothertablename:[...],...}.
  tables are visited in hash order, when each is filled in list order.
  cursor is the usual database cursor
  """
  if not data:
    data = dimsData
  tables = data.keys()
  sqlTemplate = "INSERT INTO %(table)s (%(columnList)s) VALUES (%(valueList)s)"
  for table in tables:
    what = {'table':table}
    k0 = data[table][0].keys()
    what['columnList'] = ','.join(k0)
    what['valueList'] = ','.join(["%%(%s)s"%(x) for x in k0])
    sql = sqlTemplate%(what)
    cursor.executemany(sql,data[table])
  cursor.connection.commit()

processingDays = None
productDimData = None

def fillMtbfTables(cursor, limit=12):
  global processingDays, productDimData
  cursor.execute("SELECT count(id) from productdims")
  cursor.connection.commit()
  if not cursor.fetchone()[0]:
    fillDimsTables(cursor)
  cursor.execute("SELECT p.id, p.product, p.version, p.release, o.id, o.os_name, o.os_version from productdims as p, osdims as o order by o.os_version LIMIT %s"%limit) # MUST use order by to enforce same data from run to run
  cursor.connection.rollback()
  tmpDimData = cursor.fetchall()
  productDimData = []
  for p in tmpDimData:
    p = list(p)
    if 'Linux' == p[5]:
      p[6] = "0.0.0 Linux %s GNU/Linux"%(p[6])
    productDimData.append(p)
  versionSet = set([x[2] for x in productDimData]) # lose duplicates
  versions = [x for x in versionSet][:8] # Get the right number
  #baseDate = dt.date(2008,1,1)
  baseDate = dt.datetime(2008,1,1,tzinfo=utctz)
  lintervals = [(baseDate + dt.timedelta(days=0), baseDate + dt.timedelta(days=30)),
                (baseDate + dt.timedelta(days=10),baseDate + dt.timedelta(days=40)),
                (baseDate + dt.timedelta(days=20),baseDate + dt.timedelta(days=50)),
                (baseDate + dt.timedelta(days=10),baseDate + dt.timedelta(days=40)),
                (baseDate + dt.timedelta(days=20),baseDate + dt.timedelta(days=50)),
                (baseDate + dt.timedelta(days=30),baseDate + dt.timedelta(days=60)),
                (baseDate + dt.timedelta(days=90),baseDate + dt.timedelta(days=91)),
                (baseDate + dt.timedelta(days=90),baseDate + dt.timedelta(days=91)),
                ]
  assert len(versions) >= len(lintervals), "Must add exactly %s versions to the productdims table"%(len(lintervals)-len(versions))
  assert len(lintervals) >= len(versions), "Must add exatly %s more intervals above"%(len(versions)-len(lintervals))
  intervals = {}
  for x in range(len(lintervals)):
    intervals[versions[x]] = lintervals[x]

  PDindexes = [-1,0,5,10,15,25,35,45,55,60,61]
  productsInProcessingDay = [
    [], #  -1,
    [(1,1),(1,2)],#  0,
    [(1,1),(1,2)],#  5,
    [(1,1),(1,2),(4,1),(4,2),(5,1)],#  10,
    [(1,1),(1,2),(4,1),(4,2),(5,1)],#  15,
    [(1,1),(1,2),(4,1),(4,2),(5,1),(6,1),(8,1)],#  25,
    [(2,1),(2,2),(4,1),(4,2),(5,1),(6,1),(8,1)],#  35,
    [(2,1),(2,2),(6,1),(8,1)],#  45,
    [(2,1),(2,2)],#  55,
    [(2,1),(2,2)],#  60,
    [],#  61,
    ]
  # processing days are located at and beyond the extremes of the full range, and
  # at some interior points, midway between each pair of interior points
  # layout is: (a date, the day-offset from baseDate, the expected resulting [(prod_id,os_id)])
  processingDays = [ (baseDate+dt.timedelta(days=PDindexes[x]),PDindexes[x],productsInProcessingDay[x]) for x in range(len(PDindexes))]

  # (id), productdims_id, start_date, end_date : Date-interval when product is interesting
  configData =set([(x[0],intervals[x[2]][0],intervals[x[2]][1] ) for x in productDimData ])
  cursor.execute("delete from product_visibility")
  cursor.executemany('insert into product_visibility (productdims_id,start_date,end_date) values(%s,%s,%s)',configData)
  cursor.connection.commit()
  return processingDays,productDimData

signatureData = [
  'NPSWF32.dll@0x15a4bf',
  'RtlpCoalesceFreeBlocks',
  'NPSWF32.dll@0x13b4c2',
  'nsCycleCollector::MarkRoots(GCGraphBuilder&)',
  'arena_dalloc_small | arena_dalloc',
  'kernel32.dll@0xb728',
  'RuleHash_ClassTable_GetKey',
  'SGPrxy.dll@0x258e',
  'GoogleDesktopMozilla.dll@0x54da',
  'ntdll.dll@0x19c2f',
  'user32.dll@0x8815',
  'JS_TraceChildren',
  'NPSWF32.dll@0x2166f3',
  'nanojit::Assembler::pageReset()',
  'nsEventListenerManager::Release()',
  'GraphWalker::DoWalk(nsDeque&)',
  'nsFrame::BoxReflow(nsBoxLayoutState&, nsPresContext*, nsHTMLReflowMetrics&, nsIRenderingContext*, int, int, int, int, int)',
  'NPSWF32.dll@0x172a4e',
  'NPSWF32.dll@0x20a8db',
  '@0x0',
  'NPSWF32.dll@0x2571',
  'Flash Player@0x91bd0',
  'NPSWF32.dll@0xbbff7',
  'AutoCXPusher::AutoCXPusher(JSContext*)',
  'nsChromeTreeOwner::GetInterface(nsID const&, void**)',
  'msvcr80.dll@0xf880',
  'PR_EnumerateAddrInfo',
  'nppl3260.dll@0x54bb',
   'user32.dll@0x1f793',
  'js_MonitorLoopEdge(JSContext*, unsigned int&)',
  'nsGenericElement::cycleCollection::Traverse(void*, nsCycleCollectionTraversalCallback&)',
  'ntdll.dll@0x10a19',
  'js_TraceObject',
  'NPSWF32.dll@0x5b2c8',
  'nsSocketTransport::OnSocketEvent(unsigned int, unsigned int, nsISupports*)',
  'TraceRecorder::emitIf(unsigned char*, bool, nanojit::LIns*)',
  'nsCOMPtr_base::~nsCOMPtr_base()',
  'nsHttpChannel::Release()',
  'morkRowObject::CloseRowObject(morkEnv*)',
  'NPSWF32.dll@0x1c791a',
  'isalloc',
  'user32.dll@0x11911',
  'NPSWF32.dll@0xce422',
  'nsFormFillController::SetPopupOpen',
  'nsDocShell::EnsureContentViewer()',
  'XPCCallContext::XPCCallContext(XPCContext::LangType, JSContext*, JSObject*, JSObject*, int, unsigned int, int*, int*)',
  'NPSWF32.dll@0x2cc09a',
  'libnssutil3.dylib@0x59bf',
  'memmove',
  'wcslen',
  'nppl3260.dll@0x4341',
  'nsBaseWidget::RemoveChild(nsIWidget*)',
  'JS_RemoveRootRT',
  'arena_dalloc_small',
  'nsPluginHostImpl::TrySetUpPluginInstance(char const*, nsIURI*, nsIPluginInstanceOwner*)',
  'nsCOMPtr_base::assign_from_qi(nsQueryInterface, nsID const&)',
  'NPSWF32.dll@0x3f245',
  'NPSWF32.dll@0x1c6510',
  'xul.dll@0x326f0d',
  'GoogleDesktopMozilla.dll@0x56bc',
  'js3250.dll@0x68bec',
  'arena_chunk_init',
  'PL_DHashTableOperate',
  'nsBaseWidget::Destroy()',
  'closeAudio',
  'RaiseException',
  'js_Interpret',
  'js_GetGCThingTraceKind',
  'nsDocShell::SetupNewViewer(nsIContentViewer*)',
  'iml32.dll@0x10efb',
  'UserCallWinProcCheckWow',
  'nsWindow::GetParentWindow(int)',
  'ntdll.dll@0xe514',
  'nsPluginInstancePeerImpl::GetDOMElement(nsIDOMElement**)',
  'nsStyleSet::FileRules(int (*)(nsIStyleRuleProcessor*, void*), RuleProcessorData*)',
  'user32.dll@0x1f793',
  'memcpy | fillInCell',
  'Flash_EnforceLocalSecurity',
  'objc_msgSend | IdleTimerVector',
  'msvcrt.dll@0x37fd4',
  'RtlpWaitForCriticalSection',
  'NPSWF32.dll@0x7c043',
  'nsGlobalWindow::cycleCollection::UnmarkPurple(nsISupports*)',
  'xpcom_core.dll@0x31b7a',
  'nsXPConnect::Traverse(void*, nsCycleCollectionTraversalCallback&)',
  '@0x61636f6c',
  'NPSWF32.dll@0x3d447',
  'fastcopy_I',
  'NPSWF32.dll@0xa7edc',
  'GCGraphBuilder::NoteXPCOMChild(nsISupports*)',
  'kernel32.dll@0x12afb',
  'DTToolbarFF.dll@0x4bc19',
  '_PR_MD_SEND',
  'nsXULDocument::OnStreamComplete(nsIStreamLoader*, nsISupports*, unsigned int, unsigned int, unsigned char const*)',
  '6199445655488030487637489004489057483483495843503286493177488015467495487215487445487563488802',
  'nsMimeTypeArray::GetMimeTypes()',
  'dtoa',
  'nsScriptLoader::StartLoad(nsScriptLoadRequest*, nsAString_internal const&)',
  'ntdll.dll@0x43387',
  'nsCycleCollectingAutoRefCnt::decr(nsISupports*)',
  'CFReadStreamGetStatus',
  'GoogleDesktopMozilla.dll@0x5500',
  'NPSWF32.dll@0x143c15',
  'nsXHREventTarget::GetParentObject(nsIScriptGlobalObject**)',
  'NPSWF32.dll@0x1c6168',
  'NPSWF32.dll@0x1b9cf9',
  'NSSRWLock_LockRead_Util',
  'NoteJSChild',
  'NPSWF32.dll@0x77540',
  'GoogleDesktopMozilla.dll@0x5512',
  'radhslib.dll@0x3b6f',
  'BtwVdpCapFilter.dll@0xa345',
  'ntdll.dll@0x100b',
  'avgssff.dll@0x9ba3',
  'msvcr80.dll@0xf870',
  'js_NewGCThing',
  'ntdll.dll@0x1b21a',
  'ScanBlackWalker::ShouldVisitNode(PtrInfo const*)',
  'nsAutoCompleteController::ClosePopup',
  '_PR_MD_ATOMIC_DECREMENT',
  'fun_trace',
  ]
def genSig(countOfSignatures=7):
  global signatureData
  assert countOfSignatures <= len(signatureData), 'Better be at least %s!, but was %s'%(countOfSignatures,len(signatureData))
  while True:
    for s in signatureData[:countOfSignatures]:
      yield s


def fillReportsTable(cursor, doFillMtbfTables=True, createUrls=False, numUrls=300, multiplier=1, signatureCount=7):
  """fill enough data to test mtbf and topcrashbyurl behavior:
    - mtbf: SUM(uptime); COUNT(date_processed);
    - url: some duplicates, some distinct
    createUrls: use 'actual' urls if True
    numUrls: how many of the test set to use. If 0: all of them
    multiplier: how many times to iterate the data
    signatureCount: How many distinct signatures to use
    """
  global processingDays
  if doFillMtbfTables or not processingDays:
    fillMtbfTables(cursor) # prime the pump
  sql2 = """INSERT INTO reports
                (uuid, client_crash_date, install_age, last_crash, uptime, date_processed, success, signature, url, product, version, os_name, os_version)
          VALUES(%s,   %s,                %s,          %s,         %s,     %s,             %s,      %s,        %s,  %s,      %s,      %s,      %s)"""
  processTimes = [dt.time(0,0,0),dt.time(5,0,0),  dt.time(10,0,0),dt.time(15,0,0),  dt.time(20,0,0),  dt.time(23,59,59)]
  crashTimes =   [dt.time(0,0,0),dt.time(4,59,40),dt.time(9,55,0),dt.time(14,55,55),dt.time(19,59,59),dt.time(23,59,50)]
  assert len(processTimes) == len(crashTimes), 'Otherwise things get way too weird'
  uptimes = [5*x for x in range(1,15)]
  data2 = []
  uuidGen = moreUuid()
  sigGen = genSig(signatureCount)
  urlGen = moreUrl(createUrls,numUrls)
  uptimeIndex = 0
  for mm in range(multiplier):
    for product in productDimData:
      uptime = uptimes[uptimeIndex%len(uptimes)]
      uptimeIndex += 1
      for d,off,ig in processingDays:
        for ptIndex in range(len(processTimes)):
          pt = processTimes[ptIndex].replace(microsecond = 10000*mm)
          ct = crashTimes[ptIndex].replace(microsecond = 9000*mm)
          #dp = "%s %s"%(d.isoformat(),pt.isoformat())
          dp = dt.datetime(d.year, d.month, d.day, pt.hour, pt.minute, pt.second)
          #ccd = "%s %s"%(d.isoformat(),ct.isoformat())
          ccd = dt.datetime(d.year, d.month, d.day, ct.hour, ct.minute, ct.second)
          tup = (uuidGen.next(), uptime,dp,product[1],product[2],product[5],product[6])
          tup2 = (tup[0], ccd, 10000, 100, uptime, dp, True, sigGen.next(), urlGen.next(), product[1], product[2], product[5], product[6])
          data2.append(tup2)
  cursor.executemany(sql2,data2)
  return processingDays,productDimData

def moreUrl(realUrls, count=430):
  if not realUrls:
    while True:
      yield None
  else:
    dirpath = os.path.split(os.path.abspath(__file__))[0]
    # urlLines contains over 9500 lines, many are duplicates (1999 distinct), in random order
    f = None
    try:
      f = open(os.path.join(dirpath,'urlLines.txt'))
      data = []
      if count:
        try:
          for i in range(count):
            line = f.next()
            if line:
              data.append(line.strip())
            else:
              data.append(None)
        except StopIteration:
          pass
      else:
        for line in f:
          if line:
            data.append(line.strip())
          else:
            data.append(None)
    finally:
      if f:
        f.close()
    while True:
      for url in data:
        yield url[:255]

