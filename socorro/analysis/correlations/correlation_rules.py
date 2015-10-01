import re
import os
import subprocess
import threading

from socorro.lib.transform_rules import Rule

import ujson
import re

from collections import defaultdict, Sequence, MutableMapping
from configman import Namespace, RequiredConfig
from configman.dotdict import DotDict


#==============================================================================
class ProductVersionLookup(MutableMapping):
    #--------------------------------------------------------------------------
    def __init__(self, product_version_tuples_as_string, value_type=None):
        self.original_string = product_version_tuples_as_string
        self.products = defaultdict(dict)

        pv_tuples = ujson.loads(product_version_tuples_as_string)
        for product, version in pv_tuples:
            self.products[product][version] = \
                value_type() if value_type is not None else None

    #--------------------------------------------------------------------------
    def __str__(self):
        return self.original_string

    #--------------------------------------------------------------------------
    def __getitem__(self, key):
        if isinstance(key, basestring):
            return self.products[key]
        elif isinstance(key, Sequence):
            product, version = key
            return self.products[product][version]
        else:
            raise KeyError(key)

    #--------------------------------------------------------------------------
    def __setitem__(self, key, value):
        if isinstance(key, basestring):
            raise KeyError(key)
        elif isinstance(key, Sequence):
            product, version = key
            self.products[product][version] = value
        else:
            raise KeyError(key)

    #--------------------------------------------------------------------------
    def __delitem__(self, key, value):
        if isinstance(key, basestring):
            raise KeyError(key)
        elif isinstance(key, Sequence):
            product, version = key
            del self.products[product][version]
            if not len(self.products[product]):
                del self.products[product]
        else:
            raise KeyError(key)

    #--------------------------------------------------------------------------
    def __len__(self):
        length = 0
        for version in self.products.values():
            length += len(version)
        return length

    #--------------------------------------------------------------------------
    def __iter__(self):
        for product, verisons in self.products.iteritems():
            for version in self.products[product].keys():
                yield (product, version)

    #--------------------------------------------------------------------------
    def __contains__(self, key):
        if isinstance(key, basestring):
            return key in self.products
        elif isinstance(key, Sequence):
            product, version = key
            return version in self.products[product]
        else:
            raise KeyError(key)

    #--------------------------------------------------------------------------
    def __keys__(self):
        return tuple(x for x in self)

    #--------------------------------------------------------------------------
    def __items__(self):
        return tuple((x, self[x]) for x in self)

    #--------------------------------------------------------------------------
    def __values__(self):
        return tuple(self[x] for x in self)

    #--------------------------------------------------------------------------
    def get(self, key, default=None):
        if key in self:
            return self.__getitem__(key)
        return default

    #--------------------------------------------------------------------------
    def __eq__(self, other):
        return self.__items__() == other.__items__()

    #--------------------------------------------------------------------------
    def __ne__(self, other):
        return self.__items__() != other.__items__()


#==============================================================================
class OutputStreamer(RequiredConfig):
    pass


#==============================================================================
class CorrelationRule(Rule):
    required_config = Namespace()
    required_config.add_option(
        "number_to_process",
        doc="absolute size of the dataset (0 for all)",
        default=0,
        reference_value_from='global.correlation',
    )
    required_config.add_aggregation(
        'min_crashes',
        lambda g, l: 10 if l.number_to_process > 1000 else 1
    )
    required_config.add_option(
        "release",
        doc="Process reports for product release",
        default='',
        reference_value_from='global.correlation',
    )
    required_config.add_option(
        "product_and_version_list",
        doc="list of the form [[product1, version1], [p1, v2], [p2, v3], ...]",
        default='',
        reference_value_from='global.correlation',
    )
    required_config.namespace('destination')
    required_config.destination.add_option(
        'destintation_class',
        doc="fully qualified Python path to the output",
        default=OutputStreamer
    )

    #--------------------------------------------------------------------------
    def __init__(self, config=None, quit_check_callback=None):
        super(CorrelationRule, self).__init__(config, quit_check_callback)
        self.accumulators_by_pv = ProductVersionLookup(
            config.product_and_version_list,
            DotDict
        )

    #--------------------------------------------------------------------------
    def _predicate(self, raw, dumps, processed, processor_meta):
        try:
            return (processed['product'], processed['version']) in  \
                self.accumulators_by_pv
        except KeyError:
            return False
        return False

    #--------------------------------------------------------------------------
    def save_report(self):
        summary = self.summarize()
        with self.config.destination.destintation_class(
            self.config.destination
        ) as storage:
            for item in summary:
                storage.store(item)



#==============================================================================
class CorrelationCoreCountRule(CorrelationRule):
    required_config = Namespace()
    required_config.add_option(
        "by_os_version",
        doc="Group reports by *version* of operating system",
        default=True
    )
    required_config.add_option(
        "condense",
        doc="Condense signatures in modules we don't have symbols for",
        default=True
    )

    #--------------------------------------------------------------------------
    def version(self):
        return '1.0'

    #--------------------------------------------------------------------------
    def __init__(self, config=None, quit_check_callback=None):
        super(CorrelationCoreCountRule, self).__init__(
            config,
            quit_check_callback
        )
        for an_accumulator in self.accumulators_by_pv.values():
            an_accumulator.osyses = {}


    #--------------------------------------------------------------------------
    def _action(self, raw, dumps, crash, processor_meta):
        if not "os_name" in crash:
            # We have some bad crash reports.
            return False

        # give the names of the old algorithm's critical variables to their
        # variables in the new system
        osyses = self.accumulators_by_pv[
            (crash["product"], crash["version"])
        ].osyses
        options = self.config

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # begin - original unaltered code section
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        osname = crash["os_name"]
        # The os_version field is way too specific on Linux, and we don't
        # have much Linux data anyway.
        if options.by_os_version and osname != "Linux":
            osname = osname + " " + crash["os_version"]
        osys = osyses.setdefault(osname,
                                 { "count": 0,
                                   "signatures": {},
                                   "core_counts": {} })
        signame = crash["signature"]
        if re.search(r"\S+@0x[0-9a-fA-F]+$", signame) is not None:
            if options.condense:
                # Condense all signatures in a given DLL.
                signame = re.sub(r"@0x[0-9a-fA-F]+$", "", signame)
        if "reason" in crash and crash["reason"] is not None:
            signame = signame + "|" + crash["reason"]
        signature = osys["signatures"].setdefault(signame,
                                                  { "count": 0,
                                                    "core_counts": {} })
        accumulate_objs = [osys, signature]

        for obj in accumulate_objs:
            obj["count"] = obj["count"] + 1

        if "json_dump" in crash and "system_info" in crash["json_dump"]:
            family = crash["json_dump"]["system_info"]["cpu_arch"]
            details = crash["json_dump"]["system_info"]["cpu_info"]
            cores = crash["json_dump"]["system_info"]["cpu_count"]
            infostr = family + " with " + str(cores) + " cores"
            # Increment the global count on osys and the per-signature count.
            for obj in accumulate_objs:
                obj["core_counts"][infostr] = \
                    obj["core_counts"].get(infostr, 0) + 1
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # end - original unaltered code section
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

        return True

    #--------------------------------------------------------------------------
    def _summary_for_a_product_version_pair(self, an_accumulator):
        infostr_re = re.compile("^(.*) with (\d+) cores$")

        def cmp_infostr(x, y):
            (familyx, coresx) = infostr_re.match(x).groups()
            (familyy, coresy) = infostr_re.match(y).groups()
            if familyx != familyy:
                return cmp(familyx, familyy);
            return cmp(int(coresx), int(coresy))

        pv_summary = {}
        MIN_CRASHES = self.config.min_crashes
        osyses = an_accumulator.osyses

        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        # begin - original minimally altered code section
        # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
        sorted_osyses = osyses.keys()
        sorted_osyses.sort()

        for osname in sorted_osyses:
            osys = osyses[osname]

            pv_summary[osname] = {}
##            print
##            print osname

            sorted_signatures = [sig for sig in osys["signatures"].items() \
                                 if sig[1]["count"] >= MIN_CRASHES]
            sorted_signatures.sort(key=lambda tuple: tuple[1]["count"], reverse=True)
            sorted_cores = osys["core_counts"].keys()
            sorted_cores.sort(cmp = cmp_infostr)
            for signame, sig in sorted_signatures:
                pv_summary[osname][signame] = {  # lars
                    'name': signame,
                    'count': sig['count'],
                    'cores': {},
                }
                by_number_of_cores = pv_summary[osname][signame]['cores']  # lars
##                print "  %s (%d crashes)" % (signame, sig["count"])
                for cores in sorted_cores:
                    by_number_of_cores[cores] = {}
                    in_sig_count = sig["core_counts"].get(cores, 0)
                    in_sig_ratio = float(in_sig_count) / sig["count"]
                    in_os_count = osys["core_counts"][cores]
                    in_os_ratio = float(in_os_count) / osys["count"]

                    rounded_in_sig_ratio = int(round(in_sig_ratio * 100))  # lars
                    rounded_in_os_ratio = int(round(in_os_ratio * 100))
                    by_number_of_cores[cores]['in_sig_count'] = in_sig_count
                    by_number_of_cores[cores]['in_sig_ratio'] = in_sig_ratio
                    by_number_of_cores[cores]['rounded_in_sig_ratio'] = rounded_in_sig_ratio
                    by_number_of_cores[cores]['in_os_count'] = in_os_count
                    by_number_of_cores[cores]['in_os_ratio'] = in_os_ratio
                    by_number_of_cores[cores]['rounded_in_os_ratio'] = rounded_in_os_ratio


##                    print u"    {0:3d}% ({1:d}/{2:d}) vs. {3:3d}% ({4:d}/{5:d}) {6}"\
##                          .format(int(round(in_sig_ratio * 100)),
##                                  in_sig_count,
##                                  sig["count"],
##                                  int(round(in_os_ratio * 100)),
##                                  in_os_count,
##                                  osys["count"],
##                                  cores).encode("UTF-8")
##                print
##            print
        return pv_summary

    #--------------------------------------------------------------------------
    def summarize(self):
        # for each product version pair in the accumulators
        summary = {}
        for pv, an_accumulator in self.accumulators_by_pv.iteritems():
            summary[' '.join(pv)] = self._summary_for_a_product_version_pair(
                an_accumulator
            )
        return summary

