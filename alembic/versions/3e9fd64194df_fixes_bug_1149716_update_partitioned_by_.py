"""Fixes bug 1149716 update partitioned-by-date table functions to properly handle partitioned tables

Revision ID: 3e9fd64194df
Revises: 63d05b930c3
Create Date: 2015-03-31 12:28:32.482462

"""

# revision identifiers, used by Alembic.
revision = '3e9fd64194df'
down_revision = '63d05b930c3'

from alembic import op
from socorro.lib import citexttype, jsontype, buildtype
from socorro.lib.migrations import fix_permissions, load_stored_proc

import sqlalchemy as sa
from sqlalchemy import types
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql import table, column




def upgrade():
    ### commands auto generated by Alembic - please adjust! ###
    load_stored_proc(op, [
        'update_signature_summary_architecture.sql',
        'update_signature_summary_device.sql',
        'update_signature_summary_flash_version.sql',
        'update_signature_summary_graphics.sql',
        'update_signature_summary_installations.sql',
        'update_signature_summary_os.sql',
        'update_signature_summary_process_type.sql',
        'update_signature_summary_products.sql',
        'update_signature_summary_uptime.sql',
        'find_weekly_partition.sql'
    ])
    # Insert partitioning for device as well
    op.execute("""
        INSERT INTO report_partition_info
        (table_name, build_order, keys, indexes, fkeys, partition_column, timetype)
        VALUES (
            'signature_summary_device',
            '14',
            '{"signature_id, android_device_id, product_version_id, report_date"}',
            '{report_date}',
            '{}',
            'report_date',
            'DATE'
        )
    """)
    op.execute("""
        UPDATE report_partition_info
        SET timetype = 'DATE'
        WHERE table_name = 'missing_symbols'
    """)
    ### end Alembic commands ###


def downgrade():
    ### commands auto generated by Alembic - please adjust! ###
    load_stored_proc(op, [
        'update_signature_summary_architecture.sql',
        'update_signature_summary_device.sql',
        'update_signature_summary_flash_version.sql',
        'update_signature_summary_graphics.sql',
        'update_signature_summary_installations.sql',
        'update_signature_summary_os.sql',
        'update_signature_summary_process_type.sql',
        'update_signature_summary_products.sql',
        'update_signature_summary_uptime.sql'
    ])
    op.execute(""" DROP FUNCTION find_weekly_partition(date, text) """)
    op.execute(""" delete from report_partition_info where table_name = 'signature_summary_device' """)
    op.execute("""
        UPDATE report_partition_info
        SET timetype = 'TIMESTAMPTZ'
        WHERE table_name = 'missing_symbols'
    """)
    ### end Alembic commands ###
