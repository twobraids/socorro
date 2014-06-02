# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from .unittestbase import PostgreSQLTestCase
from socorro.external.postgresql.util_service import Util
from socorro.external.postgresql.dbapi2_util import (
    execute_no_results,
)
from nose.plugins.attrib import attr
from nose.tools import eq_
from socorro.lib import datetimeutil


#==============================================================================
@attr(integration='postgres')
class TestUtil(PostgreSQLTestCase):
    """Test util service return information about versions of a product"""

    #--------------------------------------------------------------------------
    def _insert_test_data(self, connection):
        # Insert data
        self.now = datetimeutil.utc_now()
        now = self.now.date()
        execute_no_results(
            connection,
            """
            INSERT INTO products
            (product_name, sort, rapid_release_version, release_name)
            VALUES
            (
                'Firefox',
                1,
                '8.0',
                'firefox'
            ),
            (
                'Fennec',
                2,
                '11.0',
                'mobile'
            ),
            (
                'Thunderbird',
                3,
                '10.0',
                'thunderbird'
            ),
            (
                'WaterWolf',
                4,
                '100.0',
                'waterwolf'
            );
            """
        )
        execute_no_results(
            connection,
            """
            INSERT INTO release_channels
            (release_channel, sort)
            VALUES
            (
                'Release', 1
            ),
            (
                'Beta', 2
            );
            """
        )
        execute_no_results(
            connection,
            """
            INSERT INTO product_release_channels
            (product_name, release_channel, throttle)
            VALUES
            (
                'Firefox', 'Release', '0.1'
            ),
            (
                'Fennec', 'Release', '0.1'
            ),
            (
                'Fennec', 'Beta', '1.0'
            ),
            (
                'Thunderbird', 'Release', '0.1'
            ),
            (
                'WaterWolf', 'Beta', '0.1'
            );
            """
        )
        execute_no_results(
            connection,
            """
            INSERT INTO product_versions
            (product_version_id,
             product_name, major_version, release_version, version_string,
             build_date, sunset_date, featured_version, build_type,
             version_sort, is_rapid_beta, rapid_beta_id)
            VALUES
            (
                1,
                'Firefox',
                '8.0',
                '8.0',
                '8.0',
                '%(now)s',
                '%(now)s',
                False,
                'Release',
                '0008000',
                False,
                NULL
            ),
            (
                2,
                'Fennec',
                '11.0',
                '11.0',
                '11.0.1',
                '%(now)s',
                '%(now)s',
                False,
                'Release',
                '0011001',
                False,
                NULL
            ),
            (
                3,
                'Fennec',
                '12.0',
                '12.0',
                '12.0b1',
                '%(now)s',
                '%(now)s',
                False,
                'Beta',
                '00120b1',
                False,
                NULL
            ),
            (
                4,
                'Thunderbird',
                '10.0',
                '10.0',
                '10.0.2b',
                '%(now)s',
                '%(now)s',
                False,
                'Release',
                '001002b',
                False,
                NULL
            ),
            (
                5,
                'WaterWolf',
                '3.0b',
                '3.0b',
                '3.0b',
                '%(now)s',
                '%(now)s',
                False,
                'Beta',
                '003002b000',
                True,
                4
            ),
            (
                6,
                'WaterWolf',
                '3.0b',
                '3.0b',
                '3.0b1',
                '%(now)s',
                '%(now)s',
                False,
                'Beta',
                '003002b001',
                False,
                5
            ),
            (
                7,
                'WaterWolf',
                '3.0b',
                '3.0b',
                '3.0b2',
                '%(now)s',
                '%(now)s',
                False,
                'Beta',
                '003002b002',
                False,
                5
            );
            """ % {'now': now}
        )
        execute_no_results(
            connection,
            """
            INSERT INTO product_version_builds
            (build_id, platform, product_version_id)
            VALUES
            (1, 'Linux', 1),
            (2, 'Linux', 2),
            (3, 'Linux', 3),
            (4, 'Linux', 4),
            (5, 'Linux', 5),
            (6, 'Linux', 6),
            (7, 'Linux', 7);
            """
        )

    #--------------------------------------------------------------------------
    def setUp(self):
        """ Populate tables with fake data """
        super(TestUtil, self).setUp(Util)
        self.transaction(self._insert_test_data)

    #--------------------------------------------------------------------------
    def tearDown(self):
        """ Cleanup the database, delete tables and functions """
        self.transaction(
            execute_no_results,
            """
            TRUNCATE product_versions, product_version_builds,
                     products, release_channels, product_release_channels
            CASCADE;
            """
        )
        super(TestUtil, self).tearDown()

    #--------------------------------------------------------------------------
    def test_versions_info(self):
        """Test function which returns information about versions"""

        util_service = Util(config=self.config)

        # Test Firefox version
        param = {"versions": "Firefox:8.0"}

        expected = {
            'Firefox:8.0': {
                'product_version_id': 1,
                'product_name': 'Firefox',
                'version_string': '8.0',
                'major_version': '8.0',
                'release_channel': 'Release',
                'build_id': [1],
                'is_rapid_beta': False,
                'is_from_rapid_beta': False,
                'from_beta_version': 'Firefox:8.0',
            }
        }

        versions_info = util_service.get_versions_info(**param)
        eq_(versions_info, expected)

        # Test Fennec version
        param = {"versions": "Fennec:12.0b1"}

        expected = {
            'Fennec:12.0b1': {
                'product_version_id': 3,
                'product_name': 'Fennec',
                'version_string': '12.0b1',
                'major_version': '12.0',
                'release_channel': 'Beta',
                'build_id': [3],
                'is_rapid_beta': False,
                'is_from_rapid_beta': False,
                'from_beta_version': 'Fennec:12.0b1',
            }
        }

        versions_info = util_service.get_versions_info(**param)
        eq_(versions_info, expected)

        # Test empty versions
        param = {"versions": ""}
        expected = None
        versions_info = util_service.get_versions_info(**param)
        eq_(versions_info, expected)

        # Test wrong product names
        param = {"versions": ["Firefox:99.9", "Scoobidoo:99.9"]}
        expected = {}
        versions_info = util_service.get_versions_info(**param)
        eq_(versions_info, expected)

    #--------------------------------------------------------------------------
    def test_versions_info_with_rapid_betas(self):
        """Test that versions_info returns consistent data about rapid beta
        versions. """

        util_service = Util(config=self.config)

        param = {"versions": "WaterWolf:3.0b"}

        expected = {
            'WaterWolf:3.0b': {
                'product_version_id': 5,
                'product_name': 'WaterWolf',
                'version_string': '3.0b',
                'major_version': '3.0b',
                'release_channel': 'Beta',
                'build_id': [5],
                'is_rapid_beta': True,
                'is_from_rapid_beta': True,
                'from_beta_version': 'WaterWolf:3.0b',
            },
            'WaterWolf:3.0b1': {
                'product_version_id': 6,
                'product_name': 'WaterWolf',
                'version_string': '3.0b1',
                'major_version': '3.0b',
                'release_channel': 'Beta',
                'build_id': [6],
                'is_rapid_beta': False,
                'is_from_rapid_beta': True,
                'from_beta_version': 'WaterWolf:3.0b',
            },
            'WaterWolf:3.0b2': {
                'product_version_id': 7,
                'product_name': 'WaterWolf',
                'version_string': '3.0b2',
                'major_version': '3.0b',
                'release_channel': 'Beta',
                'build_id': [7],
                'is_rapid_beta': False,
                'is_from_rapid_beta': True,
                'from_beta_version': 'WaterWolf:3.0b',
            }
        }

        versions_info = util_service.get_versions_info(**param)
        eq_(versions_info, expected)
