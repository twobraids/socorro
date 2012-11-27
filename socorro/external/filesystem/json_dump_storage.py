# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import errno
import os
import shutil
import json
import collections

import socorro.external.filesystem.dump_storage as socorro_dumpStorage
import socorro.external.filesystem.filesystem as socorro_fs
import socorro.lib.util as socorro_util
import socorro.lib.ooid as socorro_ooid

from socorro.lib.datetimeutil import utc_now


class NoSuchUuidFound(Exception):
    pass


#==============================================================================
class JsonDumpStorage(socorro_dumpStorage.DumpStorage):
    """
    This class implements a file system storage scheme for the JSON and dump
    files of the Socorro project. It create a tree with two branches: the name
    branch and the date branch.
     - The name branch consists of paths based on the first 8 characters of the
       ooid file name. It holds the two data files and a relative symbolic link
       to the date branch directory associated with the particular ooid.
       see socorro.lib.ooid.py for details of date and depth encoding within
       the ooid
       For the ooid:  22adfb61-f75b-11dc-b6be-001322081225
        - the json file is stored as
          %(root)s/%(daypart)s/name/22/ad/22adfb61-f75b-11dc-b6be-001322081225
          .json
        - the dump file is stored as
          %(root)s/name/22/ad/22adfb61-f75b-11dc-b6be-001322081225.dump
        - the symbolic link is stored asnamePath
          %(root)s/name/22/ad/22adfb61-f75b-11dc-b6be-001322081225
          and (see below) references
          %(toDateFromName)s/date/2008/12/25/12/05/webhead01_0
     - The date branch consists of paths based on the year, month, day, hour,
       minute-segment, webhead host name and a small sequence number.
       For each ooid, it holds a relative symbolic link referring to the actual
       storage (name) directory holding the data for that ooid.
       For the ooid above, submitted at 2008-12-25T12:05 from webhead01
        - the symbolic link is stored as
          %(root)s/date/2008/09/30/12/05/webhead01_0/22adfb61-f75b-11dc-b6be-
          001322081225 and references %(toNameFromDate)s/name/22/ad/

    Note: The symbolic links are relative, so they begin with some rounds of
    '../'. This is to avoid issues that might arise from variously mounted nfs
    volumes. If the layout changes, self.toNameFromDate and toDateFromName
    must be changed to match, as well as a number of the private methods.

    Note: If so configured, the bottom nodes in the date path will be
    %(webheadName)s_n for n in range(N) for some reasonable (5, perhaps) N.
    Files are placed into these buckets in rotation.
    """
    #--------------------------------------------------------------------------
    def __init__(self, root=".", osModule=os, **kwargs):
        """
        Take note of our root directory and other necessities.
        Yes, it is perfectly legal to call super(...).__init__() after doing
        some other code. As long as you expect the behavior you get, anyway...
        """
        kwargs.setdefault('minutesPerSlot', 1)
        kwargs.setdefault('subSlotCount', 1)  # that is: use xxx_0 every time
                                             # by default
        super(JsonDumpStorage, self).__init__(
          root=root,
          osModule=osModule,
          **kwargs
        )
        tmp = kwargs.get('cleanIndexDirectories', 'false')
        self.cleanIndexDirectories = 'true' == tmp.lower()
        self.jsonSuffix = kwargs.get('jsonSuffix', '.json')
        if not self.jsonSuffix.startswith('.'):
            self.jsonSuffix = ".%s" % (self.jsonSuffix)
        self.dumpSuffix = kwargs.get('dumpSuffix', '.dump')
        if not self.dumpSuffix.startswith('.'):
            self.dumpSuffix = ".%s" % (self.dumpSuffix)
        self.logger = kwargs.get('logger', socorro_util.FakeLogger())

    #--------------------------------------------------------------------------
    def new_entry(self,
                  crash_id,
                  raw_crash,
                  dumps_dict,
                  webhead_host_name='webhead01',
                  timestamp=None):
        if not isinstance(dumps_dict, collections.Mapping):
            dumps_dict = {self.dump_field: dumps_dict}

        name_dir, date_dir = super(JsonDumpStorage, self).newEntry(
          crash_id,
          timestamp,
          webhead_host_name
        )

        raw_crash_pathname = os.path.join(
          name_dir,
          crash_id + self.jsonSuffix
        )
        with open(raw_crash_pathname, "w") as rcf:
            json.dump(raw_crash, rcf)

        for dump_name, dump in dumps_dict.iteritems():
            full_dump_name = self.dump_file_name(crash_id, dump_name)
            dump_pathname = os.path.join(
              name_dir,
              full_dump_name
            )
            with open(dump_pathname, "w") as dp:
                dp.write(dump)
            self.osModule.chmod(dump_pathname, self.dumpPermissions)

        name_depth = socorro_ooid.depthFromOoid(crash_id)
        if not name_depth:
            name_depth = 4
        rparts = [os.path.pardir, ] * (1 + name_depth)
        rparts.append(self.dateName)
        date_depth = 2  # .../hh/mm_slot...
        if webhead_host_name and self.subSlotCount:
            date_depth = 3  # .../webHeadName_slot
        date_parts = date_dir.split(os.path.sep)[-date_depth:]
        rparts.extend(date_parts)
        self.osModule.symlink(
          os.path.sep.join(rparts),
          os.path.join(name_dir, crash_id)
        )
        #if self.dumpGID:
            #def chown1(path):
                #self.osModule.chown(path, -1, self.dumpGID)

            #socorro_fs.visitPath(
              #self.root,
              #raw_crash_relative_pathname,
              #chown1,
              #self.osModule
            #)
            #self.osModule.chown(
              #os.path.join(nameDir, crash_id + self.dumpSuffix),
              #-1,
              #self.dumpGID
            #)
            ## socorro_fs.visitPath(self.root,
            ##   os.path.join(dateDir,ooid),
            ##   chown1
            ## )


    #--------------------------------------------------------------------------
    def newEntry(self, ooid, webheadHostName='webhead01', timestamp=None):
        """
        Sets up the name and date storage directory branches for the given ooid
        Creates any directories that it needs along the path to the appropriate
        storage location.
        Creates two relative symbolic links: the date branch link pointing to
        the name directory holding the files;
        the name branch link pointing to the date branch directory holding that
        link.
        Returns a 2-tuple containing files open for writing: (jsonfile,
        dumpfile)
        If self.dumpGID, then the file tree from root to and including the data
        files are chown'd
        If self.dumpPermissions, then chmod is called on the data files
        """
        # note: after this call, dateDir already holds link to nameDir
        nameDir, dateDir = super(JsonDumpStorage, self).newEntry(
          ooid,
          timestamp,
          webheadHostName
        )
        df, jf = None, None
        jname = os.path.join(nameDir, ooid + self.jsonSuffix)
        try:
            jf = open(jname, 'w')
        except IOError, x:
            if 2 == x.errno:
                nameDir = self.makeNameDir(ooid, timestamp)  # deliberately
                # leave this dir behind if next line throws
                jf = open(jname, 'w')
            else:
                raise x
        try:
            # Do all this in a try/finally block to unroll in case of error
            self.osModule.chmod(jname, self.dumpPermissions)
            dname = os.path.join(nameDir, ooid + self.dumpSuffix)
            df = open(dname, 'w')
            self.osModule.chmod(dname, self.dumpPermissions)
            nameDepth = socorro_ooid.depthFromOoid(ooid)
            if not nameDepth:
                nameDepth = 4
            rparts = [os.path.pardir, ] * (1 + nameDepth)
            rparts.append(self.dateName)
            dateDepth = 2  # .../hh/mm_slot...
            if webheadHostName and self.subSlotCount:
                dateDepth = 3  # .../webHeadName_slot
            dateParts = dateDir.split(os.path.sep)[-dateDepth:]
            rparts.extend(dateParts)
            self.osModule.symlink(
              os.path.sep.join(rparts),
              os.path.join(nameDir, ooid)
            )
            if self.dumpGID:

                def chown1(path):
                    self.osModule.chown(path, -1, self.dumpGID)
                socorro_fs.visitPath(
                  self.root,
                  os.path.join(nameDir, ooid + self.jsonSuffix),
                  chown1,
                  self.osModule
                )
                self.osModule.chown(
                  os.path.join(nameDir, ooid + self.dumpSuffix),
                  -1,
                  self.dumpGID
                )
                # socorro_fs.visitPath(self.root,
                #   os.path.join(dateDir,ooid),
                #   chown1
                # )
        finally:
            if not jf or not df:
                if jf:
                    jf.close()
                if df:
                    df.close()
                try:
                    self.osModule.unlink(os.path.join(dateDir, ooid))
                except:
                    pass  # ok if not there
                try:
                    self.osModule.unlink(os.path.join(nameDir, ooid))
                except:
                    pass  # ok if not there
                df, jf = None, None
        return (jf, df)

    #--------------------------------------------------------------------------
    def copyFrom(self, ooid, jsonpath, dumppath, webheadHostName, timestamp,
                 createLinks=False, removeOld=False):
        """
        Copy the two crash files from the given paths to our current storage
        location in name branch
        If createLinks, use webheadHostName and timestamp to insert links to
        and from the date branch
        If removeOld, after the files are copied, attempt to unlink the
        originals
        raises OSError if the paths are unreadable or if removeOld is true and
        either file cannot be unlinked

        """
        self.logger.debug(
          'copyFrom %s %s',
          jsonpath,
          dumppath
        )
        nameDir, nparts = self.makeNameDir(ooid, timestamp)  # deliberately
        # leave this dir behind if next line throws
        jsonNewPath = '%s%s%s%s' % (nameDir, os.sep, ooid, self.jsonSuffix)
        dumpNewPath = '%s%s%s%s' % (nameDir, os.sep, ooid, self.dumpSuffix)
        try:
            self.logger.debug(
              'about to copy json %s to %s',
              jsonpath,
              jsonNewPath
            )
            shutil.copy2(jsonpath, jsonNewPath)
        except IOError, x:
            if 2 == x.errno:
                nameDir = self.makeNameDir(ooid, timestamp)  # deliberately
                # leave this dir behind if next line throws
                self.logger.debug(
                  'oops, needed to make dir first - 2nd try copy json %s '
                  'to %s',
                  jsonpath,
                  jsonNewPath
                )
                shutil.copy2(jsonpath, jsonNewPath)
            else:
                raise x
        self.osModule.chmod(jsonNewPath, self.dumpPermissions)
        try:
            self.logger.debug(
              'about to copy dump %s to %s',
              dumppath,
              dumpNewPath
            )
            shutil.copy2(dumppath, dumpNewPath)
            self.osModule.chmod(dumpNewPath, self.dumpPermissions)
            if self.dumpGID:
                self.osModule.chown(dumpNewPath, -1, self.dumpGID)
                self.osModule.chown(jsonNewPath, -1, self.dumpGID)
        except OSError, e:
            try:
                self.osModule.unlink(jsonNewPath)
            finally:
                raise e
        if createLinks:
            self.logger.debug('building links')
            dateDir, dparts = self.makeDateDir(timestamp, webheadHostName)
            nameDepth = socorro_ooid.depthFromOoid(ooid)
            if not nameDepth:
                nameDepth = 4
            nameToDateParts = [os.pardir, ] * (1 + nameDepth)
            nameToDateParts.extend(dparts[2:])
            self.osModule.symlink(
              os.sep.join(nameToDateParts),
              os.path.join(nameDir, ooid)
            )
            try:
                dateToNameParts = [os.pardir, ] * (len(dparts) - 2)
                dateToNameParts.extend(nparts[2:])
                self.osModule.symlink(
                  os.sep.join(dateToNameParts),
                  os.path.join(dateDir, ooid)
                )
            except OSError, e:
                self.osModule.unlink(os.path.join(nameDir, ooid))
                raise e
        if removeOld:
            self.logger.debug('removing old %s, %s', jsonpath, dumppath)
            try:
                self.osModule.unlink(jsonpath)
            except OSError:
                self.logger.warning(
                  "cannot unlink Json",
                  jsonpath,
                  self.osModule.listdir(os.path.split(jsonpath)[0])
                )
                return False
            try:
                self.osModule.unlink(dumppath)
            except OSError:
                self.logger.warning(
                  "cannot unlink Dump",
                  dumppath,
                  self.osModule.listdir(os.path.split(dumppath)[0])
                )
                return False
        return True

    #--------------------------------------------------------------------------
    def transferOne(self, ooid, anotherJsonDumpStorage, createLinks=True,
                    removeOld=False, webheadHostName=None, aDate=None):
        """
        Transfer data from another JsonDumpStorage instance into this instance
        of JsonDumpStorage
        ooid - the id of the data to transfer
        anotherJsonDumpStorage - An instance of JsonDumpStorage holding the
                                 data to be transferred
        createLinks - If true, create symlinks from and to date subdir
        removeOld - If true, attempt to delete the files and symlinks in source
                    file tree
        webheadHostName: Used if known
        aDate: Used if unable to parse date from source directories and uuid
        NOTE: Assumes that the path names and suffixes for
              anotherJsonDumpStorage are the same as for self
        """
        self.logger.debug('transferOne %s %s', ooid, aDate)
        jsonFromFile = anotherJsonDumpStorage.getJson(ooid)
        self.logger.debug('fetched json')
        dumpFromFile = os.path.splitext(jsonFromFile)[0] + \
                       anotherJsonDumpStorage.dumpSuffix
        if createLinks:
            self.logger.debug('fetching stamp')
            stamp = anotherJsonDumpStorage.pathToDate(
              anotherJsonDumpStorage.lookupOoidInDatePath(None, ooid, None)[0]
            )
        else:
            self.logger.debug('not bothering to fetch stamp')
            stamp = None
        self.logger.debug('fetched pathToDate ')
        if not stamp:
            if not aDate:
                aDate = utc_now()
            stamp = aDate
        self.logger.debug('about to copyFrom ')
        self.copyFrom(
          ooid,
          jsonFromFile,
          dumpFromFile,
          webheadHostName,
          stamp,
          createLinks,
          removeOld
        )

    #--------------------------------------------------------------------------
    def getJson(self, ooid):
        """
        Returns an absolute pathname for the json file for a given ooid.
        Raises OSError if the file is missing
        """
        self.logger.debug('getJson %s', ooid)
        fname = "%s%s" % (ooid, self.jsonSuffix)
        path, parts = self.lookupNamePath(ooid)
        if path:
            fullPath = os.path.join(path, fname)
            # self.osModule.stat is moderately faster than trying to open
            # for reading
            self.readableOrThrow(fullPath)
            return fullPath
        raise OSError(errno.ENOENT, 'No such file: %s%s' % (ooid, fname))

    #--------------------------------------------------------------------------
    def getDumpAsFile(self, ooid, name=None):
        """
        Returns an absolute pathname for the dump file for a given ooid.
        Raises OSError if the file is missing
        """
        if name is None or name == self.dump_field:
            fname = ooid + self.dumpSuffix
        else:
            fname = "%s.%s%s" % (ooid, name, self.dumpSuffix)
        path, parts = self.lookupNamePath(ooid)
        msg = ('%s not stored in "%s/.../%s" file tree'
               % (ooid, self.root, self.indexName))
        if path:
            fullPath = os.path.join(path, fname)
            msg = "No such file:  %s" % (os.path.join(path, fname))
            # self.osModule.stat is moderately faster than trying to open
            # for reading
            self.readableOrThrow(fullPath)
            return fullPath
        raise OSError(errno.ENOENT, msg)

    #--------------------------------------------------------------------------
    def markAsSeen(self, ooid):
        """
        Removes the links associated with the two data files for this ooid,
        thus marking them as seen.
        Quietly returns if the ooid has no associated links.
        """
        namePath, parts = self.namePath(ooid)
        dpath = None
        try:
            dpath = os.path.join(
              namePath,
              self.osModule.readlink(os.path.join(namePath, ooid))
            )
            self.osModule.unlink(os.path.join(dpath, ooid))
        except OSError, e:
            if 2 == e.errno:  # no such file or directory
                pass
            else:
                raise e
        try:
            self.osModule.unlink(os.path.join(namePath, ooid))
        except OSError, e:
            if 2 == e.errno:  # no such file or directory
                pass
            else:
                raise e

    #--------------------------------------------------------------------------
    def destructiveDateWalk(self):
        """
        This function is a generator that yields all ooids found by walking the
        date branch of the file system.  Just before yielding a value, it
        deletes both the links (from date to name and from name to date)
        After visiting all the ooids in a given date branch, recursively
        travels up, deleting any empty subdirectories. Since the file system
        may be manipulated in a different thread, if no .json or .dump file
        is found, the links are left, and we do not yield that ooid
        """

        def handleLink(dir, name):
            nameDir = self.namePath(name)[0]
            if not self.osModule.path.isfile(
              os.path.join(nameDir, name + self.jsonSuffix)
            ):
                #print '        handleLink 1'
                return None
            if not self.osModule.path.isfile(
              os.path.join(nameDir, name + self.dumpSuffix)
            ):
                #print '        handleLink 2'
                return None
            if self.osModule.path.islink(os.path.join(nameDir, name)):
                self.osModule.unlink(os.path.join(nameDir, name))
                self.osModule.unlink(os.path.join(dir, name))
                #print '        handleLink 3'
                return name
            #print '        handleLink off end'
        dailyParts = []
        try:
            dailyParts = self.osModule.listdir(self.root)
        except OSError:
            # If root doesn't exist, quietly do nothing, eh?
            return
        for daily in dailyParts:
            #print 'daily: %s' % daily
            for dir, dirs, files in self.osModule.walk(
              os.sep.join((self.root, daily, self.dateName))
            ):
                #print dir,dirs,files
                if (os.path.split(dir)[0] ==
                    os.path.split(self.datePath(utc_now())[0])):
                    #print 'skipping dir %s' % dir
                    #print 'because: %s == %s' % (os.path.split(dir)[0],
                    #       os.path.split(self.datePath(utc_now())[0]))
                    continue
                # the links are all to (relative) directories, so we need not
                # look at files
                for d in dirs:
                    #print 'dir  ', d
                    if self.osModule.path.islink(os.path.join(dir, d)):
                        r = handleLink(dir, d)
                        #print '       r ', r
                        if r:
                            yield r
                # after finishing a given directory...
                socorro_fs.cleanEmptySubdirectories(
                  os.path.join(self.root, daily),
                  dir,
                  self.osModule
                )

    #--------------------------------------------------------------------------
    def remove(self, ooid, timestamp=None):
        """
        Removes all instances of the ooid from the file system including
          the json file, the dump file, and the two links if they still exist.
        If it finds no trace of the ooid: No links, no data files, it raises a
        NoSuchUuidFound exception.
        Attempts to remove root/daily/date subtree for empty levels above this
        date
        If self.cleanIndexDirectories, attempts to remove root/daily subtree,
        for empty levels above this name storage
        """
        namePath, nameParts = self.lookupNamePath(ooid, timestamp)
        something = 0
        if namePath:
            try:
                datePath = os.path.join(
                  namePath,
                  self.osModule.readlink(os.path.join(namePath, ooid))
                )
                if (self.osModule.path.exists(datePath)
                    and self.osModule.path.isdir(datePath)):
                    # We have a date and name path
                    self._remove(
                      ooid,
                      namePath,
                      nameParts,
                      os.path.abspath(datePath),
                      []
                    )
                    something += 1
                else:
                    raise OSError  # just to get to the next block
            except OSError:
                datePath, dateParts = \
                   self.lookupOoidInDatePath(timestamp, ooid)
                if datePath:
                    self._remove(
                      ooid,
                      namePath,
                      nameParts,
                      os.path.abspath(datePath),
                      dateParts
                    )
                    something += 1
                else:
                    print ooid, namePath, nameParts
                    self._remove(ooid, namePath, nameParts, None, [])
                    something += 1
        else:
            datePath, dateParts = self.lookupOoidInDatePath(timestamp, ooid)
            if datePath:
                try:
                    namePath = os.path.normpath(
                      self.osModule.readlink(os.path.join(datePath, ooid))
                    )
                except OSError:
                    pass
            if namePath or datePath:
                self._remove(ooid, namePath, None, datePath, dateParts)
                something += 1
        if not something:
            self.logger.warning("%s was totally unknown", ooid)
            raise NoSuchUuidFound("no trace of %s was found" % ooid)

    #--------------------------------------------------------------------------
    def _remove(self, ooid, namePath, nameParts, datePath, dateParts):
        seenCount = 0
        dailyPart = None
        if nameParts:
            dailyPart = nameParts[1]
        elif namePath:
            dailyPart = namePath.split(os.sep, 2)[1]
        elif dateParts:
            dailyPart = dateParts[1]
        elif datePath:
            dailyPart = datePath.split(os.sep, 2)[1]
        if not dailyPart:
            return
        stopper = os.path.join(self.root, dailyPart)
        # unlink on the name side first, thereby erasing any hope of removing
        # relative paths from here...
        if namePath:
            #print "*****", namePath
            #raw_crash_path = self.getJson(ooid)
            #with open(raw_crash_path) as crash_file:
                #raw_crash = json.load(crash_file)
            #dump_names = raw_crash.get('dump_names', [self.dump_field])
            #try:
                #self.osModule.unlink(os.path.join(namePath, ooid))
                #seenCount += 1
            #except:
                #pass
            files_list = [x for x in os.listdir(namePath)
                                     if x.startswith(ooid)]
            for a_file_name in files_list:
                try:
                    self.osModule.unlink(os.path.join(namePath, a_file_name))
                    seenCount += 1
                except IOError:
                    self.logger.warning("%s wasn't found", a_file_name)
            try:
                self.osModule.unlink(
                  os.path.join(namePath, ooid + self.jsonSuffix)
                )
                seenCount += 1
            except:
                pass
            if self.cleanIndexDirectories:
                try:
                    socorro_fs.cleanEmptySubdirectories(
                      stopper,
                      namePath,
                      self.osModule
                    )  # clean out name side if possible
                except OSError:
                    pass
        # and the date directory
        if datePath:
            try:
                self.osModule.unlink(os.path.join(datePath, ooid))
                seenCount += 1
            except:
                pass
            try:
                socorro_fs.cleanEmptySubdirectories(
                  self.root,
                  datePath,
                  self.osModule
                )
            except:
                pass
        if not seenCount:
            self.logger.warning("%s was totally unknown", ooid)
            raise NoSuchUuidFound("no trace of %s was found" % ooid)

    #--------------------------------------------------------------------------
    def quickDelete(self, ooid):
        """deletes just the json and dump files without testing for the links.
        This is only to be used after destructiveDateWalk that will have
        already removed the symbolic links. """
        namePath, nameParts = self.lookupNamePath(ooid)
        try:
            self.osModule.unlink(
              os.path.join(namePath, ooid + self.jsonSuffix)
            )
        except Exception:
            pass
        try:
            self.osModule.unlink(
              os.path.join(namePath, ooid + self.dumpSuffix)
            )
        except Exception:
            pass

    #--------------------------------------------------------------------------
    def dump_file_name(self, crash_id, dump_name):
        if dump_name == self.dump_field:
            return crash_id + self.dumpSuffix
        else:
            return "%s.%s%s" % (crash_id,
                                dump_name,
                                self.dumpSuffix)


