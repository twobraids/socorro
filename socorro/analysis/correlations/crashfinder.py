#!/usr/bin/python
# vim: set shiftwidth=4 tabstop=4 autoindent expandtab:
# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is find-interesting-modules.py.
#
# The Initial Developer of the Original Code is the Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2009
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
#   L. David Baron <dbaron@dbaron.org>, Mozilla Corporation (original author)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****

import tarfile
import gzip
import ujson as json
import sys
import os

__all__ = [
    "add_common_options",
    "filter_crashes",
    "all_crashes_in_tar",
    "all_crashes_in_subtree",
    "all_crashes_in_current_dataset"
]

def add_common_options(op):
    op.add_option("-s", "--small-sample",
                  action="store_true", dest="small_sample",
                  help="Use a small sample of the dataset (for testing)")
    op.add_option("-f", "--file",
                  action="store", dest="tarfile",
                  help="Tar file containing the minidumps")
    op.add_option("-r", "--release",
                  action="store", dest="release",
                  help="Process reports for product release")
    op.add_option("-p", "--product",
                  action="store", dest="product",
                  help="Process reports for product")

def filter_crashes(source, max_crashes, for_product, for_version,
                   error_if_mismatch):
    count = 0
    for crash in source:
        if for_product is not None and crash["product"] != for_product:
            if error_if_mismatch:
                raise StandardError("Unexpected product " + crash["product"])
            else:
                continue
        if for_version is not None and crash["version"] != for_version:
            if error_if_mismatch:
                raise StandardError("Unexpected version " + crash["version"])
            else:
                continue
        count = count + 1
        if max_crashes is not None and count > max_crashes:
            break
        yield crash

def all_crashes_in_tar(tarname):
    tar = tarfile.open(name=tarname, mode="r")
    # Read each file only once even if it is in the tar multiple times.
    done_files = {}
    for tarinfo in tar:
        if not tarinfo.isfile() or not tarinfo.name.endswith(".jsonz"):
            continue
        if tarinfo.name in done_files:
            continue
        done_files[tarinfo.name] = True
        io = gzip.GzipFile(fileobj=tar.extractfile(tarinfo), mode="r")
        try:
            crash = json.load(io)
        except:
            if io.tell() == 0:
                sys.stderr.write("Empty JSON in " + tarinfo.name + "\n")
            else:
                sys.stderr.write("Failed to parse JSON in " + tarinfo.name +
                                 "\n")
        io.close()
        yield crash
    tar.close()

def all_crashes_in_subtree(path):
    for root, dirs, files in os.walk(path):
        for file in files:
            if not file.endswith(".jsonz"):
                continue
            io = gzip.open(os.path.join(root, file), "rb")
            crash = json.load(io)
            io.close()
            yield crash

# A generator that gives all crashes in our current dataset.
def all_crashes_in_current_dataset(options):
    SMALL_SAMPLE_SIZE = 1000

    if (options.tarfile):
        tarfile = options.tarfile
    else:
        tarfile = "/home/dbaron/crash-stats/dump-20090929.tar"
    if (options.product):
        product = options.product
    else:
        product = "Firefox"
    if (options.release):
        release = options.release
    else:
        release = "3.5.3"
    if (options.small_sample):
        max_crashes = SMALL_SAMPLE_SIZE
    else:
        max_crashes = None
    return filter_crashes(all_crashes_in_tar(tarfile),
                          max_crashes=max_crashes,
                          for_product=product,
                          for_version=release,
                          error_if_mismatch=True)
