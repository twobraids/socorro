
# Bug 928051
# row in transform_rules table in PG
# (
#    7,
#    'processor.json_rewrite',
#    7,
#    'socorro.processor.raw_crash_rules.zte_predicate',
#    NULL,
#    NULL,
#    'socorro.processor.processor.json_reformat_action',
#    NULL,
#    'key="ReleaseChannel", format_str="release-zte"'
# )


def zte_predicate(raw_crash, processor):
    return (
        raw_crash['Android_Manufacturer'] == 'ZTE'
        and raw_crash['Android_Model'] == 'roamer2'
        and raw_crash['Android_Version'] == '15(REL)'
        and raw_crash['B2G_OS_Version'] == '1.0.1.0-prerelease'
        and raw_crash['BuildID'] in [
            '20130621133927',
            '20130621152332',
            '20130531232151',
            '20130617105829',
            '20130724040538'
        ]
        and raw_crash['ProductName'] == 'B2G'
        and raw_crash['ReleaseChannel'] == 'nightly'
        and raw_crash['Version'] == '18.0'
    )

