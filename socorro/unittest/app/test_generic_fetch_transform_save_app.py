import unittest
from mock import Mock

from configman import ConfigurationManager

from socorro.app.fetch_transform_save_app import FetchTransformSaveApp

from socorro.lib.util import DotDict, SilentFakeLogger


class TestFetchTransformSaveApp(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass


    def test_bogus_source_iter_and_worker(self):
        class TestFTSAppClass(FetchTransformSaveApp):
            def __init__(self, config):
                super(TestFTSAppClass, self).__init__(config)
                self.the_list = []

            def source_iterator(self):
                for x in xrange(5):
                    yield ((x,), {})

            def transform(self, anItem):
                self.the_list.append(anItem)
                return OK

        logger = SilentFakeLogger()
        config = DotDict({ 'logger': logger,
                           'number_of_threads': 2,
                           'maximum_queue_size': 2,
                           'source': DotDict({'crashstorage': None}),
                           'destination':DotDict({'crashstorage': None})
                         })

        fts_app = TestFTSAppClass(config)
        fts_app.main()
        self.assertTrue(len(fts_app.the_list) == 5,
                        'expected to do 5 inserts, '
                          'but %d were done instead' % len(fts_app.the_list))
        self.assertTrue(sorted(fts_app.the_list) == range(5),
                        'expected %s, but got %s' % (range(5),
                                                     sorted(fts_app.the_list)))


    def test_bogus_source_iter_and_worker(self):
        class NonInfiniteFTSAppClass(FetchTransformSaveApp):
            def source_iterator(self):
                for x in self.source.new_ooids():
                    yield ((x,), {})

        class FakeStorageSource(object):
            def __init__(self, config):
                self.store = DotDict({'1234': DotDict({'ooid': '1234',
                                                       'Product': 'FireFloozy',
                                                       'Version': '1.0'}),
                                      '1235': DotDict({'ooid': '1235',
                                                       'Product': 'ThunderRat',
                                                       'Version': '1.0'}),
                                      '1236': DotDict({'ooid': '1236',
                                                       'Product': 'Caminimal',
                                                       'Version': '1.0'}),
                                      '1237': DotDict({'ooid': '1237',
                                                       'Product': 'Fennicky',
                                                       'Version': '1.0'}),
                                     })
            def get_raw_crash(self, ooid):
                return self.store[ooid]
            def get_dump(self, ooid):
                return 'this is a fake dump'
            def new_ooids(self):
                for k in self.store.keys():
                    yield k


        class FakeStorageDestination(object):
            def __init__(self, config):
                self.store = DotDict()
                self.dumps = DotDict()
            def save_raw_crash(self, raw_crash, dump):
                self.store[raw_crash.ooid] = raw_crash
                self.dumps[raw_crash.ooid] = dump
                return OK

        logger = SilentFakeLogger()
        config = DotDict({ 'logger': logger,
                           'number_of_threads': 2,
                           'maximum_queue_size': 2,
                           'source': DotDict({'crashstorage':
                                                  FakeStorageSource}),
                           'destination':DotDict({'crashstorage':
                                                      FakeStorageDestination})
                         })

        fts_app = NonInfiniteFTSAppClass(config)
        fts_app.main()

        source = fts_app.source
        destination = fts_app.destination

        self.assertEqual(source.store, destination.store)
        self.assertEqual(len(destination.dumps), 4)
        self.assertEqual(destination.dumps['1237'], source.get_dump('1237'))


