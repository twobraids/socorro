/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

\set ON_ERROR_STOP 1

DROP TABLE IF EXISTS signature_first;

DROP TABLE IF EXISTS signature_build;

DROP TABLE IF EXISTS signature_productdims;

DROP FUNCTION IF EXISTS update_signature_matviews(timestamp, integer, integer);
